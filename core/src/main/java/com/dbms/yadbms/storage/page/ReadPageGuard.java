package com.dbms.yadbms.storage.page;

import com.dbms.yadbms.buffer.FrameHeader;
import com.dbms.yadbms.buffer.replacer.LRUKReplacer;
import com.dbms.yadbms.common.serializer.KryoSerializer;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.disk.DiskRequest;
import com.dbms.yadbms.storage.disk.DiskScheduler;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;

public class ReadPageGuard implements AutoCloseable {

  /**
   * The frame that holds the page this guard is protecting. Almost all operations of this page
   * guard should be done via this shared pointer to a `FrameHeader`.
   */
  private final FrameHeader frame;

  /** The page ID of the page we are guarding. */
  @Getter private final PageId pageId;

  /**
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  private final ReentrantLock bpmLatch;

  /**
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  private final LRUKReplacer replacer;

  /** Used when flushing pages to disk. */
  private final DiskScheduler diskScheduler;

  /** Used to serialize object to the wanted types. */
  private final KryoSerializer serializer = KryoSerializer.getInstance();

  public ReadPageGuard(
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
    frame.readLock().lock();
    frame.setPinCount(1);
  }

  public byte[] getData() {
    return frame.getData();
  }

  public void drop() {
    bpmLatch.lock();
    try {
      if (frame.unPin() == 0) {
        replacer.setEvictable(frame.getFrameId(), true);
      }
    } finally {
      bpmLatch.unlock();
    }

    frame.readLock().unlock();
  }

  public boolean isDirty() {
    return frame.isDirty();
  }

  public void flushPage() {
    if (!frame.isDirty()) {
      return;
    }

    DiskRequest request =
        DiskRequest.builder().isWrite(true).data(frame.getData()).pageId(pageId).build();
    diskScheduler.schedule(request);
    frame.clearDirty();
  }

  public <T> T getDataAs(Class<T> type) {
    return serializer.fromBytes(frame.getData(), type);
  }

  @Override
  public void close() throws Exception {
    drop();
  }
}
