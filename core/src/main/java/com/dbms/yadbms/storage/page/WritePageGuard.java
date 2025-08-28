package com.dbms.yadbms.storage.page;

import com.dbms.yadbms.buffer.FrameHeader;
import com.dbms.yadbms.buffer.replacer.LRUKReplacer;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.disk.DiskRequest;
import com.dbms.yadbms.storage.disk.DiskScheduler;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;

public class WritePageGuard implements AutoCloseable {

  private final FrameHeader frame;
  @Getter private final PageId pageId;
  private final ReentrantLock bpmLatch;
  private final LRUKReplacer replacer;
  private final DiskScheduler diskScheduler;

  public WritePageGuard(
      FrameHeader frame,
      PageId pageId,
      ReentrantLock bpmLatch,
      LRUKReplacer replacer,
      DiskScheduler diskScheduler) {
    this.frame = frame;
    this.pageId = pageId;
    this.bpmLatch = bpmLatch;
    this.replacer = replacer;
    this.diskScheduler = diskScheduler;

    frame.writeLock().lock(); // exclusive lock
    frame.setPinCount(1); // increase pin count
  }

  /** Direct access to mutable page data */
  public byte[] getDataMut() {
    return frame.getData();
  }

  public boolean isDirty() {
    return frame.isDirty();
  }

  /** Flush page to disk if dirty */
  public void flushPage() {
    if (!frame.isDirty()) {
      return;
    }

    DiskRequest request =
        DiskRequest.builder().isWrite(true).pageId(pageId).data(frame.getData()).build();

    diskScheduler.schedule(request);
    frame.clearDirty(); // assume flush success
  }

  /** Release resources (lock + unpin) */
  public void drop() {
    bpmLatch.lock();
    try {
      if (frame.unPin() == 0) {
        replacer.setEvictable(frame.getFrameId(), true);
      }
    } finally {
      bpmLatch.unlock();
    }

    frame.writeLock().unlock();
  }

  @Override
  public void close() {
    drop();
  }
}
