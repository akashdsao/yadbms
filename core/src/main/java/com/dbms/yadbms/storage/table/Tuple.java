package com.dbms.yadbms.storage.table;

import static com.dbms.yadbms.common.utils.Constants.INVALID_PAGE_ID;

import com.dbms.yadbms.catalog.Column;
import com.dbms.yadbms.catalog.Schema;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.page.RecordId;
import com.dbms.yadbms.type.Limits;
import com.dbms.yadbms.type.TypeId;
import com.dbms.yadbms.type.Value;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * -------------------------------------------------------------------------------------------------
 * | InlineCol1 | InlineCol2 | VarlenCol1_Offset | VarlenCol2_Offset | ... | VarlenPayload1 |
 * VarlenPayload2 |
 */
public class Tuple {
  private RecordId recordId;
  @Getter private byte[] data;

  public Tuple() {
    this.recordId = new RecordId(PageId.store(INVALID_PAGE_ID), 0);
    this.data = new byte[0];
  }

  public Tuple(RecordId rid) {
    this.recordId = rid;
    this.data = new byte[0];
  }

  public Tuple(List<Value> values, Schema schema) {
    if (values.size() != schema.getColumnCount()) {
      throw new IllegalArgumentException("Values size does not match schema");
    }

    // 1. Calculate tuple size: inlined + varlen payloads
    int tupleSize = schema.getInlinedStorageSize();
    for (int i : schema.getUnInlinedColumns()) {
      int len = values.get(i).getStorageSize();
      if (len == Limits.NULL_VALUE) len = 0;
      tupleSize += Integer.BYTES + len; // 4B length + payload
    }

    // 2. Allocate memory
    this.data = new byte[tupleSize];

    // 3. Serialize each attribute
    int offset = schema.getInlinedStorageSize();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      Column col = schema.getColumn(i);
      Value v = values.get(i);

      if (!col.isInlined()) {
        // write relative offset into inlined area
        ByteBuffer.wrap(data, col.getOffset(), Integer.BYTES)
            .order(ByteOrder.BIG_ENDIAN)
            .putInt(offset);

        // serialize varlen payload (len + data)
        v.serializeTo(data, offset);

        int len = v.getStorageSize();
        if (len == Limits.NULL_VALUE) len = 0;
        offset += Integer.BYTES + len;
      } else {
        // fixed-size types: write directly at column offset
        v.serializeTo(data, col.getOffset());
      }
    }
  }

  public Tuple(RecordId rid, byte[] raw, int size) {
    this.recordId = rid;
    this.data = new byte[size];
    System.arraycopy(raw, 0, this.data, 0, size); // deep copy
  }

  public Value getValue(Schema schema, int columnIdx) {
    TypeId columnType = schema.getColumn(columnIdx).getColumnType();
    byte[] dataPtr = getRawData(schema, columnIdx);
    return Value.deserializeFrom(dataPtr, columnType);
  }

  public Tuple keyFromTuple(Schema schema, Schema keySchema, List<Integer> keyAttrs) {
    List<Value> keys = new ArrayList<>();
    for (int idx : keyAttrs) {
      keys.add(this.getValue(schema, idx));
    }
    return new Tuple(keys, keySchema);
  }

  private byte[] getRawData(Schema schema, int columnIdx) {
    Column col = schema.getColumn(columnIdx);

    if (col.isInlined()) {
      int size = col.getStorageSize();
      byte[] slice = new byte[size];
      System.arraycopy(data, col.getOffset(), slice, 0, size);
      return slice;
    } else {
      // offset pointer in inlined area
      int offset =
          ByteBuffer.wrap(data, col.getOffset(), Integer.BYTES)
              .order(ByteOrder.BIG_ENDIAN)
              .getInt();
      int len = ByteBuffer.wrap(data, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();

      if (len == Limits.NULL_VALUE) {
        return new byte[0];
      }

      byte[] slice = new byte[len + Integer.BYTES];
      System.arraycopy(data, offset, slice, 0, len + Integer.BYTES);
      return slice;
    }
  }

  public void serializeTo(byte[] storage, int offset) {
    ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data.length);
    System.arraycopy(data, 0, storage, offset + Integer.BYTES, data.length);
  }

  public void deserializeFrom(byte[] storage, int offset) {
    int size = ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
    this.data = new byte[size];
    System.arraycopy(storage, offset + Integer.BYTES, this.data, 0, size);
  }

  public int getLength() {
    return data.length;
  }
}
