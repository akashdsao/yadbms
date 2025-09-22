package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** TinyIntType - 8-bit signed integer SQL type. */
public class TinyIntType extends IntegerParentType {

  public TinyIntType() {
    super(TypeId.TINYINT);
  }

  @Override
  public boolean isZero(Value val) {
    return !val.isNull() && val.asTinyInt() == 0;
  }

  // ---------- Arithmetic ----------

  @Override
  public Value add(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);

    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        {
          int result = l + right.asTinyInt();
          if (result > Byte.MAX_VALUE || result < Byte.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "TINYINT addition overflow");
          }
          return new Value(TypeId.TINYINT, (byte) result);
        }
      case SMALLINT:
        return ValueFactory.getSmallIntValue((short) (l + right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l + right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l + right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l + right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return this.add(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT add");
    }
  }

  @Override
  public Value subtract(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);

    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        {
          int result = l - right.asTinyInt();
          if (result > Byte.MAX_VALUE || result < Byte.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "TINYINT subtraction overflow");
          }
          return new Value(TypeId.TINYINT, (byte) result);
        }
      case SMALLINT:
        return ValueFactory.getSmallIntValue((short) (l - right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l - right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l - right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l - right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return this.subtract(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT subtract");
    }
  }

  @Override
  public Value multiply(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);

    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        {
          int result = l * right.asTinyInt();
          if (result > Byte.MAX_VALUE || result < Byte.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "TINYINT multiplication overflow");
          }
          return new Value(TypeId.TINYINT, (byte) result);
        }
      case SMALLINT:
        return ValueFactory.getSmallIntValue((short) (l * right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l * right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l * right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l * right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return this.multiply(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT multiply");
    }
  }

  @Override
  public Value divide(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Division by zero");

    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.TINYINT, (byte) (l / right.asTinyInt()));
      case SMALLINT:
        return ValueFactory.getSmallIntValue((short) (l / right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l / right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l / right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l / right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return this.divide(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT divide");
    }
  }

  @Override
  public Value modulo(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Modulo by zero");

    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.TINYINT, (byte) (l % right.asTinyInt()));
      case SMALLINT:
        return ValueFactory.getSmallIntValue((short) (l % right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l % right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l % right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue(l % right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return this.modulo(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT modulo");
    }
  }

  @Override
  public Value sqrt(Value val) {
    if (val.isNull()) return new Value(TypeId.DECIMAL);
    byte v = val.asTinyInt();
    if (v < 0) throw new DBException(ErrorType.INVALID_OPERATION, "Cannot sqrt negative TINYINT");
    return ValueFactory.getDecimalValue(Math.sqrt(v));
  }

  @Override
  public Value operateNull(Value left, Value right) {
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.TINYINT, Limits.INT8_NULL);
      case SMALLINT:
        return new Value(TypeId.SMALLINT, Limits.INT16_NULL);
      case INTEGER:
        return new Value(TypeId.INTEGER, Limits.INT32_NULL);
      case BIGINT:
        return new Value(TypeId.BIGINT, Limits.INT64_NULL);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, Limits.DECIMAL_NULL);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Type error in TINYINT operateNull");
    }
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l == right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l == right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l == right.asInt());
      case BIGINT:
        return CmpBool.from((long) l == right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l == right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return CmpBool.from(l == casted.asTinyInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT compareEquals");
    }
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l != right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l != right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l != right.asInt());
      case BIGINT:
        return CmpBool.from((long) l != right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l != right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return CmpBool.from(l != casted.asTinyInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT compareNotEquals");
    }
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l < right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l < right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l < right.asInt());
      case BIGINT:
        return CmpBool.from((long) l < right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l < right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return CmpBool.from(l < casted.asTinyInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT compareLessThan");
    }
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l <= right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l <= right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l <= right.asInt());
      case BIGINT:
        return CmpBool.from((long) l <= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l <= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return CmpBool.from(l <= casted.asTinyInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT compareLessThanEquals");
    }
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l > right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l > right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l > right.asInt());
      case BIGINT:
        return CmpBool.from((long) l > right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l > right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return CmpBool.from(l > casted.asTinyInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT compareGreaterThan");
    }
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    byte l = left.asTinyInt();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l >= right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l >= right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l >= right.asInt());
      case BIGINT:
        return CmpBool.from((long) l >= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l >= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.TINYINT);
        return CmpBool.from(l >= casted.asTinyInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in TINYINT compareGreaterThanEquals");
    }
  }

  // ---------- Misc ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "tinyint_null";
    return String.valueOf(val.asTinyInt());
  }

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    ByteBuffer.wrap(storage, offset, 1)
        .order(ByteOrder.BIG_ENDIAN)
        .put(val.asTinyInt());
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    byte v = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN).get();
    return new Value(TypeId.TINYINT, v);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.TINYINT);
    return new Value(TypeId.TINYINT, val.asTinyInt());
  }

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    byte v = val.asTinyInt();
    switch (targetType) {
      case TINYINT:
        return new Value(TypeId.TINYINT, v);
      case SMALLINT:
        return new Value(TypeId.SMALLINT, (short) v);
      case INTEGER:
        return new Value(TypeId.INTEGER, (int) v);
      case BIGINT:
        return new Value(TypeId.BIGINT, (long) v);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, (double) v);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, String.valueOf(v));
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "TINYINT not coercible to " + targetType);
    }
  }

  @Override
  public byte[] getData(Value val) {
    return new byte[] {val.asTinyInt()};
  }

  @Override
  public int getStorageSize(Value val) {
    return Byte.BYTES; // 1
  }
}
