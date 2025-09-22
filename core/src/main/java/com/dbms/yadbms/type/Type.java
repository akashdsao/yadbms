package com.dbms.yadbms.type;

import static com.dbms.yadbms.type.TypeId.BIGINT;
import static com.dbms.yadbms.type.TypeId.BOOLEAN;
import static com.dbms.yadbms.type.TypeId.DECIMAL;
import static com.dbms.yadbms.type.TypeId.INTEGER;
import static com.dbms.yadbms.type.TypeId.SMALLINT;
import static com.dbms.yadbms.type.TypeId.TIMESTAMP;
import static com.dbms.yadbms.type.TypeId.TINYINT;
import static com.dbms.yadbms.type.TypeId.VARCHAR;

import jdk.jshell.spi.ExecutionControl.NotImplementedException;

/** Base class for type in the Database, allows to all operations over types. */
public abstract class Type {
  protected final TypeId typeId;

  protected Type(TypeId typeId) {
    this.typeId = typeId;
  }

  // ---------------- Comparison functions ----------------
  public abstract CmpBool compareEquals(Value left, Value right);

  public abstract CmpBool compareNotEquals(Value left, Value right);

  public abstract CmpBool compareLessThan(Value left, Value right);

  public abstract CmpBool compareLessThanEquals(Value left, Value right);

  public abstract CmpBool compareGreaterThan(Value left, Value right);

  public abstract CmpBool compareGreaterThanEquals(Value left, Value right);

  // ---------------- Arithmetic & math ----------------
  public abstract Value add(Value left, Value right);

  public abstract Value subtract(Value left, Value right);

  public abstract Value multiply(Value left, Value right);

  public abstract Value divide(Value left, Value right);

  public abstract Value modulo(Value left, Value right);

  public abstract Value min(Value left, Value right);

  public abstract Value max(Value left, Value right);

  public abstract Value sqrt(Value val);

  public abstract Value operateNull(Value left, Value right);

  public abstract boolean isZero(Value val);

  // ---------------- Storage & representation ----------------
  public abstract boolean isInlined(Value val);

  public abstract String toString(Value val);

  public abstract void serializeTo(Value val, byte[] storage, int offset);

  public abstract Value deserializeFrom(byte[] storage);

  public abstract Value copy(Value val);

  public abstract Value castAs(Value val, TypeId targetType);

  public abstract byte[] getData(Value val) throws NotImplementedException;

  public abstract int getStorageSize(Value val);

  /** Get the size of this data type in bytes */
  public static int getTypeSize(TypeId typeId) {
    switch (typeId) {
      case BOOLEAN:
      case TINYINT:
        return 1;
      case SMALLINT:
        return 2;
      case INTEGER:
        return 4;
      case BIGINT:
      case DECIMAL:
      case TIMESTAMP:
        return 8;
      case VARCHAR:
      case VECTOR:
        return 0; // variable length
      default:
        throw new IllegalArgumentException("Unknown type: " + typeId);
    }
  }

  public static String typeIdToString(TypeId typeId) {
    return typeId.name();
  }

  public static Value getMinValue(TypeId typeId) {
    switch (typeId) {
      case BOOLEAN:
        return new Value(TypeId.BOOLEAN, false);
      case TINYINT:
        return new Value(TypeId.TINYINT, Limits.INT8_MIN);
      case SMALLINT:
        return new Value(TypeId.SMALLINT, Limits.INT16_MIN);
      case INTEGER:
        return new Value(TypeId.INTEGER, Limits.INT32_MIN);
      case BIGINT:
        return new Value(TypeId.BIGINT, Limits.INT64_MIN);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, Limits.DECIMAL_MIN);
      case TIMESTAMP:
        return new Value(TypeId.TIMESTAMP, Limits.TIMESTAMP_MIN);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, ""); // minimal string = empty string
      default:
        throw new IllegalArgumentException("Cannot get minimal value for " + typeId);
    }
  }

  public static Value getMaxValue(TypeId typeId) {
    switch (typeId) {
      case BOOLEAN:
        return new Value(TypeId.BOOLEAN, true);
      case TINYINT:
        return new Value(TypeId.TINYINT, Limits.INT8_MAX);
      case SMALLINT:
        return new Value(TypeId.SMALLINT, Limits.INT16_MAX);
      case INTEGER:
        return new Value(TypeId.INTEGER, Limits.INT32_MAX);
      case BIGINT:
        return new Value(TypeId.BIGINT, Limits.INT64_MAX);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, Limits.DECIMAL_MAX);
      case TIMESTAMP:
        return new Value(TypeId.TIMESTAMP, Limits.TIMESTAMP_MAX);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, (String) null); // represent unbounded max as null
      default:
        throw new IllegalArgumentException("Cannot get maximal value for " + typeId);
    }
  }
}
