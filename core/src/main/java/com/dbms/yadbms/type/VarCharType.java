package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** VarCharType - VARCHAR implementation. Variable-length, not inlined. */
public class VarCharType extends Type {

  public VarCharType() {
    super(TypeId.VARCHAR);
  }

  // ---------- Data access ----------

  @Override
  public byte[] getData(Value val) {
    if (val.isNull()) return new byte[0];
    return val.asString().getBytes(); // encode as UTF-8 by default
  }

  @Override
  public int getStorageSize(Value val) {
    if (val.isNull()) return 0;
    return val.asString().getBytes().length + 1; // +1 for null terminator
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asString().compareTo(right.castAs(TypeId.VARCHAR).asString()) == 0);
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asString().compareTo(right.castAs(TypeId.VARCHAR).asString()) != 0);
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asString().compareTo(right.castAs(TypeId.VARCHAR).asString()) < 0);
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asString().compareTo(right.castAs(TypeId.VARCHAR).asString()) <= 0);
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asString().compareTo(right.castAs(TypeId.VARCHAR).asString()) > 0);
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asString().compareTo(right.castAs(TypeId.VARCHAR).asString()) >= 0);
  }

  // ---------- Aggregates ----------

  @Override
  public Value min(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    return (left.compareLessThan(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  @Override
  public Value max(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    return (left.compareGreaterThan(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  // ---------- Debug ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "varlen_null";
    return val.asString();
  }

  // ---------- Storage ----------

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    if (val.isNull()) {
      ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(-1);
      return;
    }
    byte[] data = val.asString().getBytes();
    ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data.length);
    System.arraycopy(data, 0, storage, offset + Integer.BYTES, data.length);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    ByteBuffer buffer = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN);
    int len = buffer.getInt();
    if (len == -1) {
      return new Value(TypeId.VARCHAR, (String) null);
    }
    byte[] data = new byte[len - 1];
    buffer.get(data);
    buffer.get(); // consume terminator
    return new Value(TypeId.VARCHAR, new String(data));
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.VARCHAR, (String) null);
    return new Value(TypeId.VARCHAR, val.asString());
  }

  // ---------- Casting ----------

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    String str = val.asString();
    switch (targetType) {
      case BOOLEAN:
        {
          String low = str.toLowerCase();
          if (low.equals("true") || low.equals("1") || low.equals("t")) {
            return new Value(TypeId.BOOLEAN, true);
          }
          if (low.equals("false") || low.equals("0") || low.equals("f")) {
            return new Value(TypeId.BOOLEAN, false);
          }
          throw new DBException(ErrorType.INVALID_OPERATION, "Invalid boolean string: " + str);
        }
      case TINYINT:
        try {
          byte tiny = Byte.parseByte(str);
          return new Value(TypeId.TINYINT, tiny);
        } catch (NumberFormatException e) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Numeric value out of range for TINYINT");
        }
      case SMALLINT:
        try {
          short s = Short.parseShort(str);
          return new Value(TypeId.SMALLINT, s);
        } catch (NumberFormatException e) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Numeric value out of range for SMALLINT");
        }
      case INTEGER:
        try {
          int i = Integer.parseInt(str);
          return new Value(TypeId.INTEGER, i);
        } catch (NumberFormatException e) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Numeric value out of range for INTEGER");
        }
      case BIGINT:
        try {
          long l = Long.parseLong(str);
          return new Value(TypeId.BIGINT, l);
        } catch (NumberFormatException e) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Numeric value out of range for BIGINT");
        }
      case DECIMAL:
        try {
          double d = Double.parseDouble(str);
          return new Value(TypeId.DECIMAL, d);
        } catch (NumberFormatException e) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Numeric value out of range for DECIMAL");
        }
      case VARCHAR:
        return this.copy(val);
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "VARCHAR not coercible to " + targetType);
    }
  }

  // ---------- Required overrides ----------

  @Override
  public boolean isZero(Value val) {
    return false;
  }

  @Override
  public Value add(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Add not supported for VARCHAR");
  }

  @Override
  public Value subtract(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Subtract not supported for VARCHAR");
  }

  @Override
  public Value multiply(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Multiply not supported for VARCHAR");
  }

  @Override
  public Value divide(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Divide not supported for VARCHAR");
  }

  @Override
  public Value modulo(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Modulo not supported for VARCHAR");
  }

  @Override
  public Value sqrt(Value val) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Sqrt not supported for VARCHAR");
  }

  @Override
  public Value operateNull(Value left, Value right) {
    return new Value(TypeId.VARCHAR, (String) null);
  }

  @Override
  public boolean isInlined(Value val) {
    return false;
  }
}
