package com.dbms.yadbms.common.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Unit tests for the Channel<T> class. */
class ChannelTest {

  @Test
  void testSingleThreadPutGet() throws InterruptedException {
    Channel<String> channel = new Channel<>();
    String message = "test";
    channel.put(message);
    String result = channel.get();
    assertEquals(message, result, "Channel should return the same element that was put");
    assertTrue(channel.isEmpty(), "Channel should be empty after get");
  }

  @Test
  void testGetBlocksUntilPut() throws Exception {
    Channel<Integer> channel = new Channel<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Integer> future =
          executor.submit(
              () -> {
                try {
                  return channel.get();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw e;
                }
              });

      // Give the consumer some time to block on get()
      Thread.sleep(100);

      int value = 42;
      channel.put(value);

      Integer result = future.get(1, TimeUnit.SECONDS);
      assertEquals(value, result, "get() should unblock and return the put value");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testMultipleProducersConsumers() throws Exception {
    Channel<Integer> channel = new Channel<>();
    int producerCount = 2;
    int messagesPerProducer = 50;
    ExecutorService producerPool = Executors.newFixedThreadPool(producerCount);

    List<Integer> produced = Collections.synchronizedList(new ArrayList<>());

    // Start producers
    for (int i = 0; i < producerCount; i++) {
      final int offset = i * 100;
      producerPool.submit(
          () -> {
            for (int j = 1; j <= messagesPerProducer; j++) {
              int msg = offset + j;
              channel.put(msg);
              produced.add(msg);
              Thread.yield();
            }
          });
    }

    producerPool.shutdown();
    assertTrue(
        producerPool.awaitTermination(1, TimeUnit.SECONDS), "Producers did not finish in time");

    // Consume all messages
    List<Integer> consumed = new ArrayList<>();
    for (int i = 0; i < producerCount * messagesPerProducer; i++) {
      consumed.add(channel.get());
    }

    // Sort and compare
    List<Integer> expected = new ArrayList<>(produced);
    Collections.sort(expected);
    Collections.sort(consumed);
    assertEquals(expected, consumed, "All produced messages should be consumed");
  }

  @Test
  void testGetInterrupted() throws Exception {
    Channel<Integer> channel = new Channel<>();
    Thread consumer =
        new Thread(
            () -> {
              try {
                channel.get();
                fail("Expected InterruptedException when thread is interrupted while waiting");
              } catch (InterruptedException e) {
                // expected
              }
            });
    consumer.start();

    // Give the thread time to block
    Thread.sleep(100);
    consumer.interrupt();
    consumer.join(1000);

    assertFalse(consumer.isAlive(), "Consumer thread should terminate after interruption");
  }
}
