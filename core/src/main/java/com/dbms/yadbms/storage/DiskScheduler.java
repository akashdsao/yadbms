package com.dbms.yadbms.storage;

import com.dbms.yadbms.common.utils.Channel;
import com.dbms.yadbms.config.PageId;


public class DiskScheduler {

    private final Channel<DiskRequest> requestChannel;

    private final DiskManager diskManager;

    private final Thread backgroundThread;

    private final DiskRequest POISON_PILL = new DiskRequest.DiskRequestBuilder().isWrite(false).data(null).pageId(new PageId(-1)).build();

    public DiskScheduler(DiskManager diskManager) {
        this.diskManager = diskManager;

        requestChannel = new Channel<>();

        backgroundThread = new Thread(this::startWorkerThread);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }

    public void schedule(DiskRequest req) {
        requestChannel.put(req);
    }

    public void startWorkerThread() {
        try {
            while (true) {
                DiskRequest request = requestChannel.get();

                if (request == POISON_PILL) {
                    break;
                }
                processRequest(request);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processRequest(DiskRequest request) {
        try {
            if (request.isWrite()) {
                diskManager.writePage(request.getPageId(), request.getData());
            } else {
                diskManager.readPage(request.getPageId(), request.getData());
            }
            request.getCallback().complete(true);
        } catch (Exception e) {
            request.getCallback().completeExceptionally(e);
        }
    }

    public void shutDown() {
        schedule(POISON_PILL);
        try {
            backgroundThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
