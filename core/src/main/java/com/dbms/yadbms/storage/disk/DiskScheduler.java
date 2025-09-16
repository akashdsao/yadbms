package com.dbms.yadbms.storage.disk;

import com.dbms.yadbms.common.utils.Channel;
import com.dbms.yadbms.config.PageId;

/**
 * DiskScheduler is responsible for scheduling disk read/write requests. It uses a background thread
 * to process requests asynchronously. All reads stops when a poison pill is received or the
 * scheduler is shut down or the pageId overflows.
 */
public class DiskScheduler {

  private final Channel<DiskRequest> requestChannel;

  private final DiskManager diskManager;

  private final Thread backgroundThread;

  private final DiskRequest poisonPill =
      DiskRequest.builder().isWrite(false).data(null).pageId(new PageId(Integer.MAX_VALUE)).build();

  public DiskScheduler(DiskManager diskManager) {
    this.diskManager = diskManager;

    requestChannel = new Channel<>();
    backgroundThread = new Thread(this::startWorkerThread);
    backgroundThread.setName("DiskScheduler-Worker-Thread");
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

        if (request == poisonPill) {
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
    schedule(poisonPill);
    try {
      backgroundThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
