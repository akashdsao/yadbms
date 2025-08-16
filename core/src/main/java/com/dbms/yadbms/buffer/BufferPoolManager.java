package com.dbms.yadbms.buffer;

import com.dbms.yadbms.buffer.replacer.LRUKReplacer;
import com.dbms.yadbms.config.FrameId;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.disk.DiskManager;
import com.dbms.yadbms.storage.disk.DiskRequest;
import com.dbms.yadbms.storage.disk.DiskScheduler;
import com.dbms.yadbms.storage.page.ReadPageGuard;
import com.dbms.yadbms.storage.page.WritePageGuard;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.dbms.yadbms.config.Constants.LRU_REPLACER_K;

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
            FrameId frameId;

            //Case 1: Page already in memory
            if (pageTable.containsKey(pageId)) {
                frameId = pageTable.get(pageId);
                replacer.recordAccess(frameId);
                replacer.setEvictable(frameId, false);
                ReadPageGuard pageGuard = new ReadPageGuard(frames.get(frameId.getValue()), pageId, bpmLock, replacer, diskScheduler);
                return Optional.of(pageGuard);
            }

            //Case 2: Use free frames if possible
            if (!freeFrames.isEmpty()) {
                frameId = freeFrames.poll();
            }
            // Case 3: If there is no free frame create space for it
            else {
                Optional<FrameId> evictedFrame = replacer.evict();
                if (evictedFrame.isEmpty()) {
                    return Optional.empty();
                }
                frameId = evictedFrame.get();
                FrameHeader victim = frames.get(frameId.getValue());
                if (victim.isDirty()) {
                    DiskRequest flushRequest = DiskRequest.builder().isWrite(true).pageId(victim.getPageId()).data(victim.getData()).build();
                    diskScheduler.schedule(flushRequest);
                }
                pageTable.remove(victim.getPageId());
            }
            FrameHeader frame = frames.get(frameId.getValue());
            DiskRequest readRequest = DiskRequest.builder().isWrite(false).pageId(pageId).data(frame.getData()).build();
            diskScheduler.schedule(readRequest);
            frame.setPageId(pageId);
            frame.clearDirty();
            pageTable.put(pageId, frameId);
            replacer.recordAccess(frameId);
            return Optional.of(new ReadPageGuard(frame, pageId, bpmLock, replacer, diskScheduler));
        } finally {
            bpmLock.unlock();
        }
    }

    public Optional<WritePageGuard> checkedPageWrite(PageId pageId) {
        bpmLock.lock();
        try {
            FrameId frameId;

            // Case 1: Page already in memory
            if (pageTable.containsKey(pageId)) {
                frameId = pageTable.get(pageId);
                replacer.recordAccess(frameId);
                replacer.setEvictable(frameId, false);
                WritePageGuard pageGuard = new WritePageGuard(frames.get(frameId.getValue()), pageId, bpmLock, replacer, diskScheduler);
                return Optional.of(pageGuard);
            }

            // Case 2: Use free frames if possible
            if (!freeFrames.isEmpty()) {
                frameId = freeFrames.poll();
            }
            // Case 3: If there is no free frame create space for it
            else {
                Optional<FrameId> evictedFrame = replacer.evict();
                if (evictedFrame.isEmpty()) {
                    return Optional.empty();
                }
                frameId = evictedFrame.get();
                FrameHeader victim = frames.get(frameId.getValue());
                if (victim.isDirty()) {
                    DiskRequest flushRequest = DiskRequest.builder().isWrite(true).pageId(victim.getPageId()).data(victim.getData()).build();
                    diskScheduler.schedule(flushRequest);
                }
                pageTable.remove(victim.getPageId());
            }
            FrameHeader frame = frames.get(frameId.getValue());
            DiskRequest writeRequest = DiskRequest.builder().isWrite(true).pageId(pageId).data(frame.getData()).build();
            diskScheduler.schedule(writeRequest);
            frame.setPageId(pageId);
            frame.clearDirty();
            pageTable.put(pageId, frameId);
            replacer.recordAccess(frameId);
            return Optional.of(new WritePageGuard(frame, pageId, bpmLock, replacer, diskScheduler));
        } finally {
            bpmLock.unlock();
        }
    }
}
