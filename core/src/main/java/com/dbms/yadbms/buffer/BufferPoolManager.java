package com.dbms.yadbms.buffer;

import static com.dbms.yadbms.common.utils.Constants.LRU_REPLACER_K;

import com.dbms.yadbms.buffer.replacer.LRUKReplacer;
import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.FrameId;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.disk.DiskManager;
import com.dbms.yadbms.storage.disk.DiskRequest;
import com.dbms.yadbms.storage.disk.DiskScheduler;
import com.dbms.yadbms.storage.page.ReadPageGuard;
import com.dbms.yadbms.storage.page.WritePageGuard;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufferPoolManager {
  private final int numFrames;

  private final DiskManager diskManager;

  private final int kDistance = LRU_REPLACER_K;

  private final ReentrantLock bpmLock;

  private PageId nextPageId;

  private final List<FrameHeader> frames;

  private final Map<PageId, FrameId> pageTable;

  private final Queue<FrameId> freeFrames;

  private final LRUKReplacer replacer;

  private final DiskScheduler diskScheduler;

  public BufferPoolManager(int numFrames, DiskManager diskManager) {
    this.numFrames = numFrames;
    this.diskManager = diskManager;
    this.bpmLock = new ReentrantLock();
    this.nextPageId = PageId.store(0);
    this.frames = new ArrayList<>(numFrames);
    this.pageTable = new HashMap<>(numFrames);
    this.freeFrames = new ArrayDeque<>(0);

    for (int i = 0; i < numFrames; i++) {
      frames.add(new FrameHeader(FrameId.store(i)));
      freeFrames.add(FrameId.store(i));
    }

    this.replacer = new LRUKReplacer(numFrames);
    this.diskScheduler = new DiskScheduler(diskManager);
  }

  public int size() {
    return numFrames;
  }

  public Optional<ReadPageGuard> checkedReadPage(PageId pageId) {
    bpmLock.lock();
    try {
      // Case 1: already resident
      FrameId frameId = pageTable.get(pageId);
      if (frameId != null) {
        touchForUse(frameId);
        return Optional.of(
            new ReadPageGuard(
                frames.get(frameId.getValue()), pageId, bpmLock, replacer, diskScheduler));
      }

      // Case 2: need a frame (free or evicted), then read from disk
      Optional<FrameId> acquiredFrameId = acquireFrameId();
      if (acquiredFrameId.isEmpty()) return Optional.empty();

      FrameId fid = acquiredFrameId.get();
      FrameHeader frameHeader = frames.get(fid.getValue());

      // Bring page from disk into this frame
      DiskRequest read =
          DiskRequest.builder().isWrite(false).pageId(pageId).data(frameHeader.getData()).build();
      diskScheduler.schedule(read);

      frameHeader.setPageId(pageId);
      frameHeader.clearDirty();
      pageTable.put(pageId, fid);
      touchForUse(fid);

      return Optional.of(new ReadPageGuard(frameHeader, pageId, bpmLock, replacer, diskScheduler));
    } finally {
      bpmLock.unlock();
    }
  }

  public Optional<WritePageGuard> checkedPageWrite(PageId pageId) {
    bpmLock.lock();
    try {
      // Case 1: already resident
      FrameId frameId = pageTable.get(pageId);
      if (frameId != null) {
        touchForUse(frameId);
        return Optional.of(
            new WritePageGuard(
                frames.get(frameId.getValue()), pageId, bpmLock, replacer, diskScheduler));
      }

      // Case 2: need a frame, then read existing page from disk
      Optional<FrameId> acquiredFrameId = acquireFrameId();
      if (acquiredFrameId.isEmpty()) return Optional.empty();

      FrameId fid = acquiredFrameId.get();
      FrameHeader frameHeader = frames.get(fid.getValue());

      DiskRequest read =
          DiskRequest.builder().isWrite(false).pageId(pageId).data(frameHeader.getData()).build();
      diskScheduler.schedule(read);

      frameHeader.setPageId(pageId);
      frameHeader.clearDirty();
      pageTable.put(pageId, fid);
      touchForUse(fid);

      return Optional.of(new WritePageGuard(frameHeader, pageId, bpmLock, replacer, diskScheduler));
    } finally {
      bpmLock.unlock();
    }
  }

  /** Allocate a brand-new, zeroed page and return a write guard pinned to it. */
  public PageId newPage() {
    bpmLock.lock();
    try {
      Optional<FrameId> acquireFrameId = acquireFrameId();
      if (acquireFrameId.isEmpty()) {
        log.error("All frames pinned; can't allocate new page");
        System.exit(1);
      }

      FrameId fid = acquireFrameId.get();
      FrameHeader frameHeader = frames.get(fid.getValue());

      // Assign fresh PageId (increment AFTER using current value)
      PageId newPid = nextPageId;
      nextPageId = PageId.store(newPid.getValue() + 1);

      // Zero the frame; no disk I/O needed yet
      Arrays.fill(frameHeader.getData(), (byte) 0);
      frameHeader.setPageId(newPid);
      frameHeader.clearDirty();
      frameHeader.setPinCount(0); // guard constructors will raise pin to 1

      pageTable.put(newPid, fid);
      touchForUse(fid);

      return newPid;
    } finally {
      bpmLock.unlock();
    }
  }

  /** Get a usable frame: either a free one, or evict one (flushing if needed). */
  private Optional<FrameId> acquireFrameId() {
    FrameId frameId = freeFrames.poll();
    if (frameId != null) return Optional.of(frameId);

    Optional<FrameId> evicted = replacer.evict();
    if (evicted.isEmpty()) return Optional.empty();
    FrameId victimId = evicted.get();
    FrameHeader victim = frames.get(victimId.getValue());
    flushIfDirty(victim);

    pageTable.remove(victim.getPageId());

    return Optional.of(victimId);
  }

  /** Flush a dirty frame to disk. Caller must hold bpmLock. */
  private void flushIfDirty(FrameHeader fh) {
    if (!fh.isDirty()) return;
    DiskRequest flushRequest =
        DiskRequest.builder().isWrite(true).pageId(fh.getPageId()).data(fh.getData()).build();
    diskScheduler.schedule(flushRequest);
    fh.clearDirty();
  }

  /** Common “touch” when a frame is (re)used. */
  private void touchForUse(FrameId frameId) {
    replacer.recordAccess(frameId);
    replacer.setEvictable(frameId, false);
  }

  /**
   * A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
   *
   * <p>If `CheckedWritePage` returns a null pointer it raises an expection and break the process.
   *
   * @param pageId The ID of the page we want to read.
   * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
   */
  public WritePageGuard writePage(PageId pageId) {
    Optional<WritePageGuard> writePageGuard = checkedPageWrite(pageId);
    if (writePageGuard.isEmpty()) {
      throw new DBException(ErrorType.IO_ERROR, "Write page failed for pageId " + pageId);
    }
    return writePageGuard.get();
  }

  /**
   * A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
   * If`CheckedReadPage` returns a null pointer, it raises an exception and breaks the process.
   *
   * @param pageId The ID of the page we want to read.
   * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
   */
  public ReadPageGuard readPage(PageId pageId) {
    Optional<ReadPageGuard> readPageGuard = checkedReadPage(pageId);
    if (readPageGuard.isEmpty()) {
      throw new DBException(ErrorType.IO_ERROR, "Read page failed for pageId " + pageId);
    }
    return readPageGuard.get();
  }
}
