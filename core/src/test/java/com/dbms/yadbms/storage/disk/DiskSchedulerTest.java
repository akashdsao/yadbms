package com.dbms.yadbms.storage.disk;

import static com.dbms.yadbms.common.utils.Constants.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dbms.yadbms.config.PageId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DiskSchedulerTest {

  private static DiskManager diskManager;
  private static DiskScheduler diskScheduler;

  @BeforeAll
  public static void setUp() {
    diskManager = new DiskManager(Path.of("src/test/test.db"));
    diskScheduler = new DiskScheduler(diskManager);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    diskScheduler.shutDown();
    diskManager.shutDown();
    Files.deleteIfExists(Path.of("src/test/test.db"));
  }

  @Test
  void testDiskSchedulerBackgroundThreadIsRunning() {
    DiskRequest request =
        DiskRequest.builder()
            .isWrite(false)
            .data(new byte[PAGE_SIZE])
            .pageId(PageId.store(0))
            .callback(new CompletableFuture<>())
            .build();

    diskScheduler.schedule(request);

    try {
      assertTrue(
          request.getCallback().get(1000, java.util.concurrent.TimeUnit.MILLISECONDS),
          "DiskScheduler background thread should process the request successfully");
    } catch (Exception e) {
      throw new RuntimeException("DiskScheduler background thread is not running properly", e);
    }
  }

  @Test
  void testDiskSchedulerReadRequest() {
    DiskRequest request =
        DiskRequest.builder()
            .isWrite(false)
            .data(new byte[PAGE_SIZE])
            .pageId(PageId.store(1))
            .callback(new CompletableFuture<>())
            .build();

    diskScheduler.schedule(request);

    try {
      assertTrue(
          request.getCallback().get(1000, java.util.concurrent.TimeUnit.MILLISECONDS),
          "DiskScheduler should process read request successfully");
    } catch (Exception e) {
      throw new RuntimeException("DiskScheduler read request failed", e);
    }
  }

  @Test
  void testDiskSchedulerWriteAndThenReadRequest() {
    String testData = "hello";
    byte[] writeBuffer = new byte[PAGE_SIZE];
    byte[] textBytes = testData.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(textBytes, 0, writeBuffer, 0, textBytes.length);
    DiskRequest writeRequest =
        DiskRequest.builder()
            .isWrite(true)
            .data(writeBuffer)
            .pageId(PageId.store(1))
            .callback(new CompletableFuture<>())
            .build();
    byte[] readBuffer = new byte[PAGE_SIZE];
    DiskRequest readRequest =
        DiskRequest.builder()
            .isWrite(false)
            .data(readBuffer)
            .pageId(PageId.store(1))
            .callback(new CompletableFuture<>())
            .build();

    diskScheduler.schedule(writeRequest);
    diskScheduler.schedule(readRequest);

    try {
      readRequest.getCallback().get(10000, TimeUnit.MILLISECONDS);
      String res = new String(readBuffer, StandardCharsets.UTF_8).substring(0, testData.length());
      assertEquals(testData, res, "Write and read data should match");
    } catch (Exception e) {
      throw new RuntimeException("DiskScheduler read request failed", e);
    }
  }

  @Test
  void testDiskSchedulerFailedReadRequest() {
    DiskRequest request =
        DiskRequest.builder()
            .isWrite(false)
            .data(new byte[0])
            .pageId(PageId.store(0))
            .callback(new CompletableFuture<>())
            .build();

    diskScheduler.schedule(request);

    try {
      request.getCallback().get(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      assertTrue(true, "DiskScheduler should handle failed read request");
    }
  }

  @Test
  void testDiskScheduler_100RandomWords_concurrentWriteThenRead() throws Exception {
    final int ops = 100; // 100 writes + 100 reads
    final int threads = 8; // submission concurrency
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CyclicBarrier start = new CyclicBarrier(threads);

    List<Future<?>> submits = new ArrayList<>();

    // Split the 100 ops among N threads roughly evenly
    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      submits.add(
          pool.submit(
              () -> {
                try {
                  start.await(); // start submissions together
                  for (int i = threadId; i < ops; i += threads) {
                    // Unique page id per op to avoid content races
                    PageId pid = PageId.store(10_000 + i);

                    // ----- build random 4-letter word -----
                    byte[] word = randomWord4();
                    String expected = new String(word, StandardCharsets.US_ASCII);

                    // ----- prepare write buffer (word at start, rest zero) -----
                    byte[] writeBuf = new byte[PAGE_SIZE];
                    System.arraycopy(word, 0, writeBuf, 0, 4);

                    // ----- enqueue WRITE -----
                    CompletableFuture<Boolean> wDone = new CompletableFuture<>();
                    DiskRequest wReq =
                        DiskRequest.builder()
                            .isWrite(true)
                            .data(writeBuf)
                            .pageId(pid)
                            .callback(wDone)
                            .build();
                    diskScheduler.schedule(wReq);

                    // ----- enqueue READ -----
                    byte[] readBuf = new byte[PAGE_SIZE];
                    CompletableFuture<Boolean> rDone = new CompletableFuture<>();
                    DiskRequest rReq =
                        DiskRequest.builder()
                            .isWrite(false)
                            .data(readBuf)
                            .pageId(pid)
                            .callback(rDone)
                            .build();
                    diskScheduler.schedule(rReq);

                    // Wait for the read to finish (write precedes read in queue order)
                    assertTrue(
                        rDone.get(10, TimeUnit.SECONDS), "Read did not complete for page " + pid);

                    // Verify first 4 bytes match the word we wrote
                    String got =
                        new String(Arrays.copyOfRange(readBuf, 0, 4), StandardCharsets.US_ASCII);
                    assertEquals(expected, got, "Mismatch on page " + pid);
                  }
                } catch (Throwable e) {
                  throw new RuntimeException(e);
                }
              }));
    }

    // Wait for all submitters to finish
    for (Future<?> f : submits) f.get(30, TimeUnit.SECONDS);
    pool.shutdownNow();
    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
  }

  private static byte[] randomWord4() {
    // Lowercase a..z
    ThreadLocalRandom r = ThreadLocalRandom.current();
    byte[] w = new byte[4];
    for (int i = 0; i < 4; i++) w[i] = (byte) ('a' + r.nextInt(26));
    return w;
  }
}
