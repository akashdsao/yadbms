package com.dbms.yadbms.storage.page;

import static com.dbms.yadbms.common.utils.Constants.INVALID_PAGE_ID;
import static com.dbms.yadbms.common.utils.Constants.PAGE_SIZE;
import static com.dbms.yadbms.common.utils.Constants.TABLE_PAGE_HEADER_SIZE;
import static com.dbms.yadbms.common.utils.Constants.TUPLE_INFO_SIZE;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.storage.table.Tuple;
import com.dbms.yadbms.storage.table.TupleInfo;
import com.dbms.yadbms.storage.table.TupleMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;

/**
 * Slotted page format:
 *
 * <pre>
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 * </pre>
 *
 * Header format (size in bytes):
 *
 * <pre>
 *  ----------------------------------------------------------------------------
 *  | NextPageId (4) | NumTuples (2) | NumDeletedTuples (2) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Tuple_1 offset+size (4) | Tuple_2 offset+size (4) | ... |
 *  ----------------------------------------------------------------
 * </pre>
 *
 * Tuple format:
 *
 * <pre>
 *  | meta | data |
 * </pre>
 */
public class TablePage {
  @Getter @Setter private int nextPageId;
  @Getter private int numTuples;
  private int numDeletedTuples;
  private final List<TupleInfo> tupleInfo;
  private final byte[] pageStart;

  public TablePage() {
    this.pageStart = new byte[PAGE_SIZE];
    this.tupleInfo = new ArrayList<>();
    init();
  }

  /** Initialize the TablePage header */
  public void init() {
    this.nextPageId = INVALID_PAGE_ID;
    this.numTuples = 0;
    this.numDeletedTuples = 0;
    this.tupleInfo.clear();
  }

  /** Compute next available offset for a given tuple. */
  public Optional<Integer> getNextTupleOffset(TupleMetaData meta, Tuple tuple) {
    int slotEndOffset;
    if (numTuples > 0) {
      TupleInfo last = tupleInfo.get(numTuples - 1);
      slotEndOffset = last.getOffset();
    } else {
      slotEndOffset = PAGE_SIZE;
    }

    int tupleOffset = slotEndOffset - tuple.getLength();
    int offsetSize = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (numTuples + 1);

    if (tupleOffset < offsetSize) {
      return Optional.empty(); // not enough space
    }
    return Optional.of(tupleOffset);
  }

  /** Insert tuple into this page */
  public Optional<Integer> insertTuple(TupleMetaData meta, Tuple tuple) {
    Optional<Integer> tupleOffsetCalculated = getNextTupleOffset(meta, tuple);
    if (tupleOffsetCalculated.isEmpty()) {
      return Optional.empty();
    }
    int tupleOffset = tupleOffsetCalculated.get();
    int tupleId = numTuples;

    // record in directory
    tupleInfo.add(new TupleInfo(tupleOffset, tuple.getLength(), meta));
    numTuples++;

    // copy bytes into page storage
    System.arraycopy(tuple.getData(), 0, pageStart, tupleOffset, tuple.getLength());

    return Optional.of(tupleId);
  }

  /** Update metadata only */
  public void updateTupleMeta(TupleMetaData meta, RecordId rid) {
    int tupleId = rid.getSlotNumber();
    if (tupleId >= numTuples) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Tuple ID out of range");
    }
    TupleInfo slot = tupleInfo.get(tupleId);
    if (!slot.getTupleMetaData().isDeleted() && meta.isDeleted()) {
      numDeletedTuples++;
    }
    slot.setTupleMetaData(meta);
  }

  /** Get a tuple */
  public Map<TupleMetaData, Tuple> getTuple(RecordId rid) {
    int tupleId = rid.getSlotNumber();
    if (tupleId >= numTuples) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Tuple ID out of range");
    }
    TupleInfo slot = tupleInfo.get(tupleId);

    byte[] tupleBytes = new byte[slot.getSize()];
    System.arraycopy(pageStart, slot.getOffset(), tupleBytes, 0, slot.getSize());

    Tuple t = new Tuple(rid, tupleBytes, slot.getSize());
    return Map.of(slot.getTupleMetaData(), t);
  }

  /** Get tuple meta only */
  public TupleMetaData getTupleMetaData(RecordId rid) {
    int tupleId = rid.getSlotNumber();
    if (tupleId >= numTuples) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Tuple ID out of range");
    }
    return tupleInfo.get(tupleId).getTupleMetaData();
  }

  /** Update a tuple in place (unsafe if size mismatch) */
  public void updateTupleInPlaceUnsafe(TupleMetaData meta, Tuple tuple, RecordId rid) {
    int tupleId = rid.getSlotNumber();
    if (tupleId >= numTuples) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Tuple ID out of range");
    }
    TupleInfo slot = tupleInfo.get(tupleId);

    if (slot.getSize() != tuple.getLength()) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Tuple size mismatch");
    }
    if (!slot.getTupleMetaData().isDeleted() && meta.isDeleted()) {
      numDeletedTuples++;
    }

    slot.setTupleMetaData(meta);
    System.arraycopy(tuple.getData(), 0, pageStart, slot.getOffset(), tuple.getLength());
  }
}
