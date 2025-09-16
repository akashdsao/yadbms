package com.dbms.yadbms.buffer;

import static com.dbms.yadbms.common.utils.Constants.PAGE_SIZE;

import com.dbms.yadbms.config.FrameId;
import com.dbms.yadbms.config.PageId;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.Setter;

/**
 * FrameHeader represents the metadata for a frame in the buffer pool. It contains information such
 * as the frame ID, page ID, pin count, dirty status, and the actual data of the frame.
 */
public class FrameHeader {
  @Getter private final FrameId frameId;

  @Getter @Setter private PageId pageId;

  private final ReentrantReadWriteLock readWriteLock;

  private AtomicInteger pinCount;

  private boolean isDirty;

  @Getter private byte[] data;

  public FrameHeader(FrameId frameId) {
    this.frameId = frameId;
    readWriteLock = new ReentrantReadWriteLock();
    reset();
  }

  public void reset() {
    pinCount = new AtomicInteger(0);
    isDirty = false;
    data = new byte[PAGE_SIZE];
    pageId = null;
  }

  public ReentrantReadWriteLock.ReadLock readLock() {
    return readWriteLock.readLock();
  }

  public ReentrantReadWriteLock.WriteLock writeLock() {
    return readWriteLock.writeLock();
  }

  public void setPinCount(int pinCount) {
    this.pinCount.set(pinCount);
  }

  public int pin() {
    return pinCount.incrementAndGet();
  }

  public boolean isDirty() {
    return isDirty;
  }

  public void clearDirty() {
    isDirty = false;
  }

  public int unPin() {
    if (pinCount.get() > 0) {
      return pinCount.decrementAndGet();
    }
    return pinCount.get();
  }
}
