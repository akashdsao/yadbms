package com.dbms.yadbms.storage.disk;

import com.dbms.yadbms.config.PageId;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class DiskRequest {
  private boolean isWrite;
  private byte[] data;
  private PageId pageId;
  private CompletableFuture<Boolean> callback;
}
