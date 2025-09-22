package com.dbms.yadbms.catalog;

import java.util.*;
import lombok.Getter;

/** Schema - holds metadata for a row schema (list of columns). */
public class Schema {
  @Getter private final List<Column> columns;
  private final int length;
  private final boolean tupleIsInlined;
  @Getter private final List<Integer> unInlinedColumns;

  /** Construct schema from a list of columns. */
  public Schema(List<Column> cols) {
    this.columns = cols;
    int currentOffset = 0;
    List<Integer> unInlined = new ArrayList<>();
    boolean allInlined = true;

    for (int i = 0; i < columns.size(); i++) {
      Column col = columns.get(i);
      if (!col.isInlined()) {
        unInlined.add(i);
        allInlined = false;
      }
      col.setColumnOffset(currentOffset);
      if (col.isInlined()) {
        currentOffset += col.getStorageSize();
      } else {
        currentOffset += Integer.BYTES;
      }
    }
    this.length = currentOffset;
    this.unInlinedColumns = Collections.unmodifiableList(unInlined);
    this.tupleIsInlined = allInlined;
  }

  /** Copy a schema by selecting a subset of columns. */
  public static Schema copySchema(Schema from, List<Integer> attrs) {
    List<Column> cols = new ArrayList<>();
    for (int idx : attrs) {
      cols.add(from.getColumn(idx));
    }
    return new Schema(cols);
  }

  /**
   * @return column at index
   */
  public Column getColumn(int colIdx) {
    return columns.get(colIdx);
  }

  /**
   * @return index of column by name (throws if not found)
   */
  public int getColIdx(String colName) {
    return tryGetColIdx(colName)
        .orElseThrow(() -> new NoSuchElementException("Column does not exist: " + colName));
  }

  /**
   * @return optional index of column by name
   */
  public Optional<Integer> tryGetColIdx(String colName) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(colName)) {
        return Optional.of(i);
      }
    }
    return Optional.empty();
  }

  /**
   * @return number of columns
   */
  public int getColumnCount() {
    return columns.size();
  }

  /**
   * @return number of uninlined columns
   */
  public int getUninlinedColumnCount() {
    return unInlinedColumns.size();
  }

  /**
   * @return number of bytes used by one tuple (fixed part only)
   */
  public int getInlinedStorageSize() {
    return length;
  }

  /**
   * @return true if all columns are inlined
   */
  public boolean isInlined() {
    return tupleIsInlined;
  }

  public String toString(boolean simplified) {
    StringBuilder sb = new StringBuilder();
    if (simplified) {
      sb.append("Schema[");
      for (int i = 0; i < columns.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append(columns.get(i).toString(true));
      }
      sb.append("]");
    } else {
      sb.append("Schema[");
      for (int i = 0; i < columns.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append(columns.get(i).toString(false));
      }
      sb.append("] length=").append(length).append(" inlined=").append(tupleIsInlined);
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return toString(true);
  }
}
