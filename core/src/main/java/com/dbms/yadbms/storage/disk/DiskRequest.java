package com.dbms.yadbms.storage.disk;

import com.dbms.yadbms.config.PageId;
import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;

@Builder
@Getter
public class DiskRequest {
    private boolean isWrite;
    private byte[] data;
    private PageId pageId;
    private CompletableFuture<Boolean> callback;
}
