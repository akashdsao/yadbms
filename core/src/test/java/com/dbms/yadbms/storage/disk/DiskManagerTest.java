package com.dbms.yadbms.storage.disk;

import static com.dbms.yadbms.common.utils.Constants.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.dbms.yadbms.config.PageId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DiskManagerTest {
  private static final Path dbFilePath = Path.of("src/test/test.db");

  private static DiskManager diskManager;

  @BeforeAll
  public static void setUp() {
    if (Files.exists(dbFilePath)) {
      try {
        Files.delete(dbFilePath);
      } catch (Exception e) {
        throw new RuntimeException("Failed to delete existing test database file", e);
      }
    }
    diskManager = new DiskManager(dbFilePath);
  }

  @AfterAll
  public static void tearDown() {
    diskManager.shutDown();
    try {
      Files.deleteIfExists(dbFilePath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete test database file", e);
    }
  }

  @Test
  void testReadWritePage() {
    byte[] data = new byte[PAGE_SIZE];
    byte[] buf = new byte[PAGE_SIZE];
    Arrays.fill(data, (byte) 42); // test content

    PageId page0 = PageId.store(0);
    PageId page5 = PageId.store(5);

    // Read uninitialized page (tolerate empty read)
    diskManager.readPage(page0, buf);
    // May be all 0s or undefined — we don't assert here

    // Write page 0 and read it back
    diskManager.writePage(page0, data);
    Arrays.fill(buf, (byte) 0);
    diskManager.readPage(page0, buf);
    assertArrayEquals(data, buf, "Page 0 data should match after write and read");

    // Write page 5 and read it back
    diskManager.writePage(page5, data);
    Arrays.fill(buf, (byte) 0);
    diskManager.readPage(page5, buf);
    assertArrayEquals(data, buf, "Page 5 data should match after write and read");
  }
}
