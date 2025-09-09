package com.dbms.yadbms.storage.disk;

import static com.dbms.yadbms.common.utils.Constants.DEFAULT_DB_IO_SIZE;
import static com.dbms.yadbms.common.utils.Constants.PAGE_SIZE;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.PageId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * DiskManager is responsible for managing the database file and log file on disk. It handles
 * reading and writing pages, allocating new pages, and managing free slots. It also provides
 * methods to write and read logs.
 */
@Slf4j
public class DiskManager {

  private final Path dbFilePath;
  private final Path logFilePath;

  private final FileChannel dbChannel;
  private final FileChannel logChannel;

  private long pageCapacity = DEFAULT_DB_IO_SIZE;

  @Getter private long numWrites;

  @Getter private long numDeletes;

  @Getter private long numFlushes;

  /** records the pageId vs offset */
  private final Map<PageId, Long> pages;

  /** records the free slots in the DB file if pages are deleted, indicated by offset; */
  private final Deque<Long> freeSlots;

  /**
   * Constructs a DiskManager with the specified database file path. Initializes the log file and
   * database file channels, and sets up a shutdown hook to close them on exit.
   *
   * @param dbFilePath the path to the database file
   */
  public DiskManager(Path dbFilePath) {
    this.dbFilePath = dbFilePath;
    String logFileName = dbFilePath.getFileName().toString().replaceFirst("\\.[^.]+$", "") + ".log";
    this.logFilePath = dbFilePath.getParent().resolve(logFileName);

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutDown));

    try {
      // Initialize log file channel
      if (!Files.exists(logFilePath)) {
        Files.createFile(logFilePath);
      }
      logChannel = FileChannel.open(logFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      logChannel.position(logChannel.size()); // move to end if appending

      // Initialize DB file channel
      ReentrantLock dbIOLock = new ReentrantLock();
      dbIOLock.lock();
      try {
        if (!Files.exists(dbFilePath)) {
          Files.createFile(dbFilePath);
        }
        dbChannel = FileChannel.open(dbFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);

        long requiredSize = (pageCapacity + 1) * PAGE_SIZE;
        if (Files.size(dbFilePath) < requiredSize) {
          dbChannel.truncate(requiredSize);
        }

      } finally {
        dbIOLock.unlock();
      }

      pages = new HashMap<>();
      freeSlots = new ArrayDeque<>();
      numWrites = 0;
      numDeletes = 0;
      numFlushes = 0;
    } catch (IOException e) {
      throw new DBException(ErrorType.IO_ERROR, "DiskManager initialization failed", e);
    }
  }

  /** Shuts down the DiskManager by closing the database and log file channels. */
  public synchronized void shutDown() {
    try {
      if (dbChannel != null && dbChannel.isOpen()) {
        dbChannel.close();
      }
      if (logChannel != null && logChannel.isOpen()) {
        logChannel.close();
      }
    } catch (IOException e) {
      log.error("❌ Error closing channels: {}", e.getMessage());
    }
  }

  /**
   * Writes a page to the database file at the specified PageId. If the page does not exist, it
   * allocates a new page.
   *
   * @param pageId the PageId of the page to write
   * @param pageData the data to write to the page
   */
  public synchronized void writePage(PageId pageId, byte[] pageData) {
    long offset =
        pages.computeIfAbsent(
            pageId,
            id -> {
              try {
                return allocatePage();
              } catch (IOException e) {
                throw new DBException(
                    ErrorType.IO_ERROR, "Failed to allocate page for pageId: " + pageId.toString());
              }
            });

    try {
      ByteBuffer buffer = ByteBuffer.wrap(pageData);
      int bytesWritten = dbChannel.write(buffer, offset);

      if (bytesWritten == -1) {
        log.error("Failed to write to {} pageId", pageId.getValue());
      }
      numWrites++;
      pages.put(pageId, offset);
      dbChannel.force(true);
    } catch (IOException e) {
      throw new DBException(ErrorType.IO_ERROR, "Failed to write to pageId: " + pageId.toString());
    }
  }

  /**
   * Reads a page from the database file at the specified PageId. If the page does not exist, it
   * allocates a new page, lazily initializing it for logical consistency.
   *
   * @param pageId the PageId of the page to read
   * @param pageData the byte array to store the read data
   */
  public synchronized void readPage(PageId pageId, byte[] pageData) {
    try {
      long offset =
          pages.computeIfAbsent(
              pageId,
              id -> {
                try {
                  return allocatePage();
                } catch (IOException e) {
                  throw new DBException(
                      ErrorType.IO_ERROR,
                      "Failed to allocate page for pageId: " + pageId.toString());
                }
              });

      long fileSize = Files.size(dbFilePath);
      if (offset > fileSize) {
        log.error("I/O error: Read page {} past the end of file at offset {}", pageId, offset);
        return;
      }

      pages.put(pageId, offset); // store the mapping

      ByteBuffer buffer = ByteBuffer.wrap(pageData);
      int bytesRead = dbChannel.read(buffer, offset);

      if (bytesRead == -1) {
        log.error("I/O error: Unable to read page {} at offset {}", pageId, offset);
        return;
      }

      if (bytesRead < PAGE_SIZE) {
        log.error("Partial read: Page {} read {} bytes, filling rest with 0s", pageId, bytesRead);
        Arrays.fill(pageData, bytesRead, PAGE_SIZE, (byte) 0);
      }

    } catch (IOException e) {
      log.error("I/O exception while reading page {}: {}", pageId, e.getMessage());
    }
  }

  /**
   * Deletes a page from the database file at the specified PageId. The page is marked as deleted by
   * adding its offset to the free slots.
   *
   * @param pageId the PageId of the page to delete
   */
  public synchronized void deletePage(PageId pageId) {
    if (!pages.containsKey(pageId)) {
      return;
    }
    freeSlots.addLast(pages.get(pageId));
    pages.remove(pageId);
    numDeletes++;
  }

  /**
   * Writes a log entry to the log file. The log entry is appended to the end of the log file and
   * flushed to disk.
   *
   * @param logData the byte array containing the log data
   * @param size the size of the log data to write
   */
  public synchronized void writeLog(byte[] logData, int size) {
    try {
      // Write to log file
      ByteBuffer buffer = ByteBuffer.wrap(logData, 0, size);
      logChannel.write(buffer); // append at current position

      // Force flush to disk
      logChannel.force(true);

      numFlushes += 1;
    } catch (IOException e) {
      log.error("I/O error while writing log: {}", e.getMessage());
    }
  }

  /**
   * Reads a log entry from the log file at the specified offset. The log entry is read into the
   * provided byte array.
   *
   * @param logData the byte array to store the read log data
   * @param size the size of the log data to read
   * @param offset the offset in the log file to read from
   * @return true if the read was successful, false otherwise
   */
  public synchronized boolean readLog(byte[] logData, int size, int offset) {
    try {
      long fileSize = Files.size(logFilePath);
      if (offset > fileSize) {
        log.error("I/O error: Read log at offset {}", offset);
        return false;
      }
      ByteBuffer buffer = ByteBuffer.wrap(logData, 0, size);
      int bytesRead = logChannel.read(buffer, offset);

      if (bytesRead == -1) {
        log.error("I/O error: Unable to read log at offset {}", offset);
        return false;
      }

      if (bytesRead < size) {
        log.error("Partial read:  for log file size {} and bytes read {}", size, bytesRead);
        Arrays.fill(logData, bytesRead, PAGE_SIZE, (byte) 0);
      }
      return true;
    } catch (IOException e) {
      log.error("Unable to read log: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Allocates a new page in the database file. If there are free slots available, it reuses one of
   * them. If the current page capacity is exceeded, it doubles the capacity and truncates the file
   * accordingly.
   *
   * @return the offset of the allocated page
   * @throws IOException if an I/O error occurs while allocating the page
   */
  public long allocatePage() throws IOException {
    if (!freeSlots.isEmpty()) {
      long offset = freeSlots.getLast();
      freeSlots.removeLast();
      return offset;
    }

    if (pages.size() > pageCapacity) {
      pageCapacity *= 2;
      long requiredSize = (pageCapacity + 1) * PAGE_SIZE;
      dbChannel.truncate(requiredSize);
    }

    return (long) pages.size() * PAGE_SIZE;
  }
}
