package com.dbms.yadbms.storage.table;

import com.dbms.yadbms.buffer.BufferPoolManager;
import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.page.ReadPageGuard;
import com.dbms.yadbms.storage.page.RecordId;
import com.dbms.yadbms.storage.page.TablePage;
import com.dbms.yadbms.storage.page.WritePageGuard;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

public class TableHeap {
  @Getter private final BufferPoolManager bufferPoolManager;
  PageId firstPageId;
  PageId lastPageId;

  TableHeap(BufferPoolManager bufferPoolManager) {
    this.bufferPoolManager = bufferPoolManager;
    this.firstPageId = bufferPoolManager.newPage();
    this.lastPageId = firstPageId;
    try (WritePageGuard guard = bufferPoolManager.writePage(firstPageId)) {
      TablePage firstPage = guard.asMut(TablePage.class);
      firstPage.init();
    }
  }

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return empty.
   *
   * @param metaData tuple metadata
   * @param tuple tuple to insert
   * @return rid of the inserted tuple
   */
  public synchronized Optional<RecordId> insertTuple(TupleMetaData metaData, Tuple tuple) {
    try (WritePageGuard guard = bufferPoolManager.writePage(lastPageId)) {
      while (true) {
        TablePage page = guard.asMut(TablePage.class);
        if (page.getNextTupleOffset(metaData, tuple).isEmpty()) {
          break;
        }
        PageId nextPageId = bufferPoolManager.newPage();
        page.setNextPageId(nextPageId.getValue());
        try (WritePageGuard nextPageGuard = bufferPoolManager.writePage(nextPageId)) {
          TablePage nextPage = nextPageGuard.asMut(TablePage.class);
          nextPage.init();
          lastPageId = nextPageId;
        }
      }
      TablePage page = guard.asMut(TablePage.class);
      Optional<Integer> slotId = page.insertTuple(metaData, tuple);
      if (slotId.isEmpty()) {
        throw new DBException(ErrorType.IO_ERROR, "Failed to insert tuple");
      }
      return Optional.of(new RecordId(lastPageId, slotId.get()));
    }
  }

  /**
   * Update the meta of a tuple.
   *
   * @param metaData new tuple meta
   * @param rid the rid of the inserted tuple
   */
  public void updateTupleMetaData(TupleMetaData metaData, RecordId rid) {
    try (WritePageGuard pageGuard = bufferPoolManager.writePage(rid.getPageId())) {
      TablePage page = pageGuard.asMut(TablePage.class);
      page.updateTupleMeta(metaData, rid);
    }
  }

  /**
   * Read a tuple from the table.
   *
   * @param rid rid of the tuple to read
   * @return the meta and tuple
   */
  public Map<TupleMetaData, Tuple> getTuple(RecordId rid) {
    try (ReadPageGuard pageGuard = bufferPoolManager.readPage(rid.getPageId())) {
      TablePage page = pageGuard.asMut(TablePage.class);
      return page.getTuple(rid);
    }
  }

  /**
   * Read a tuple meta from the table. Note: if you want to get tuple and meta together, use
   * `GetTuple` instead to ensure atomicity.
   *
   * @param rid rid of the tuple to read
   * @return the metadata of the tuple
   */
  public TupleMetaData getTupleMetaData(RecordId rid) {
    try (ReadPageGuard pageGuard = bufferPoolManager.readPage(rid.getPageId())) {
      TablePage page = pageGuard.asMut(TablePage.class);
      return page.getTupleMetaData(rid);
    }
  }
}
