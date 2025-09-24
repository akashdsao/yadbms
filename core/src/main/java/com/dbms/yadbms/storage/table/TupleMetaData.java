package com.dbms.yadbms.storage.table;

import lombok.Getter;

/** Metadata for a tuple (timestamp / txn id + deletion marker). */
public class TupleMetaData {

  /** the ts / txn_id of this tuple. */
  @Getter private final long ts;

  /** marks whether this tuple is marked removed from table heap. */
  private final boolean isDeleted;

  public static final long INVALID_TS = -1;

  public TupleMetaData(long ts, boolean isDeleted) {
    this.ts = ts;
    this.isDeleted = isDeleted;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof TupleMetaData)) return false;
    TupleMetaData other = (TupleMetaData) obj;
    return this.ts == other.ts && this.isDeleted == other.isDeleted;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(ts);
    result = 31 * result + Boolean.hashCode(isDeleted);
    return result;
  }

  @Override
  public String toString() {
    return "TupleMeta{ts=" + ts + ", isDeleted=" + isDeleted + "}";
  }
}
