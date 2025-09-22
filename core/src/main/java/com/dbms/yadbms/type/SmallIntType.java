package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** SmallIntType - 16-bit signed integer SQL type. */
public class SmallIntType extends IntegerParentType {

  public SmallIntType() {
    super(TypeId.SMALLINT);
  }

  @Override
  public boolean isZero(Value val) {
    return !val.isNull() && val.asSmallInt() == 0;
  }

  // ---------- Arithmetic ----------

  @Override
  public Value add(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);

    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        {
          int r = right.asSmallInt();
          int result = l + r;
          if (result > Short.MAX_VALUE || result < Short.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "SMALLINT addition overflow");
          }
          return new Value(TypeId.SMALLINT, (short) result);
        }
      case INTEGER:
        return ValueFactory.getIntegerValue(l + right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l + right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l + right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return this.add(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT add");
    }
  }

  @Override
  public Value subtract(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);

    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        {
          int r = right.asSmallInt();
          int result = l - r;
          if (result > Short.MAX_VALUE || result < Short.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "SMALLINT subtraction overflow");
          }
          return new Value(TypeId.SMALLINT, (short) result);
        }
      case INTEGER:
        return ValueFactory.getIntegerValue(l - right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l - right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l - right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return this.subtract(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT subtract");
    }
  }

  @Override
  public Value multiply(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);

    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        {
          int r = right.asSmallInt();
          int result = l * r;
          if (result > Short.MAX_VALUE || result < Short.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "SMALLINT multiplication overflow");
          }
          return new Value(TypeId.SMALLINT, (short) result);
        }
      case INTEGER:
        return ValueFactory.getIntegerValue(l * right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l * right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l * right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return this.multiply(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT multiply");
    }
  }

  @Override
  public Value divide(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Division by zero");

    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return new Value(TypeId.SMALLINT, (short) (l / right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l / right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l / right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l / right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return this.divide(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT divide");
    }
  }

  @Override
  public Value modulo(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Modulo by zero");

    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return new Value(TypeId.SMALLINT, (short) (l % right.asSmallInt()));
      case INTEGER:
        return ValueFactory.getIntegerValue(l % right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l % right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue(l % right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return this.modulo(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT modulo");
    }
  }

  @Override
  public Value sqrt(Value val) {
    if (val.isNull()) return new Value(TypeId.DECIMAL);
    short v = val.asSmallInt();
    if (v < 0) throw new DBException(ErrorType.INVALID_OPERATION, "Cannot sqrt negative SMALLINT");
    return ValueFactory.getDecimalValue(Math.sqrt(v));
  }

  @Override
  public Value operateNull(Value left, Value right) {
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return new Value(TypeId.SMALLINT, Limits.INT16_NULL);
      case INTEGER:
        return new Value(TypeId.INTEGER, Limits.INT32_NULL);
      case BIGINT:
        return new Value(TypeId.BIGINT, Limits.INT64_NULL);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, Limits.DECIMAL_NULL);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Type error in SMALLINT operateNull");
    }
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return CmpBool.from(l == right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l == right.asInt());
      case BIGINT:
        return CmpBool.from((long) l == right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l == right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return CmpBool.from(l == casted.asSmallInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT compareEquals");
    }
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return CmpBool.from(l != right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l != right.asInt());
      case BIGINT:
        return CmpBool.from((long) l != right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l != right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return CmpBool.from(l != casted.asSmallInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT compareNotEquals");
    }
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return CmpBool.from(l < right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l < right.asInt());
      case BIGINT:
        return CmpBool.from((long) l < right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l < right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return CmpBool.from(l < casted.asSmallInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT compareLessThan");
    }
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return CmpBool.from(l <= right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l <= right.asInt());
      case BIGINT:
        return CmpBool.from((long) l <= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l <= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return CmpBool.from(l <= casted.asSmallInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT compareLessThanEquals");
    }
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return CmpBool.from(l > right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l > right.asInt());
      case BIGINT:
        return CmpBool.from((long) l > right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l > right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return CmpBool.from(l > casted.asSmallInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT compareGreaterThan");
    }
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    short l = left.asSmallInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
        return CmpBool.from(l >= right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l >= right.asInt());
      case BIGINT:
        return CmpBool.from((long) l >= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l >= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.SMALLINT);
        return CmpBool.from(l >= casted.asSmallInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in SMALLINT compareGreaterThanEquals");
    }
  }

  // ---------- Misc ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "smallint_null";
    return String.valueOf(val.asSmallInt());
  }

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    short v = val.asSmallInt();
    ByteBuffer.wrap(storage, offset, Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(v);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    short v = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN).getShort();
    return new Value(TypeId.SMALLINT, v);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.SMALLINT);
    return new Value(TypeId.SMALLINT, val.asSmallInt());
  }

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    short v = val.asSmallInt();
    switch (targetType) {
      case TINYINT:
        if (v > Limits.INT8_MAX || v < Limits.INT8_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for TINYINT");
        }
        return new Value(TypeId.TINYINT, (int) v);
      case SMALLINT:
        return new Value(TypeId.SMALLINT, v);
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
            ErrorType.INVALID_OPERATION, "SMALLINT not coercible to " + targetType);
    }
  }

  @Override
  public byte[] getData(Value val) {
    ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN);
    buffer.putShort(val.asSmallInt());
    return buffer.array();
  }

  @Override
  public int getStorageSize(Value val) {
    return Short.BYTES; // 2
  }
}
