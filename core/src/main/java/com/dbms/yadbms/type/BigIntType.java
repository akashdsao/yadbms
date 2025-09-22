package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BigIntType extends IntegerParentType {

  public BigIntType() {
    super(TypeId.BIGINT);
  }

  @Override
  public boolean isZero(Value val) {
    return !val.isNull() && val.asBigInt() == 0L;
  }

  // ---------- Arithmetic ----------

  @Override
  public Value add(Value left, Value right) {
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getBigIntValue(l + right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue(l + right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l + right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return this.add(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT add");
    }
  }

  @Override
  public Value subtract(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getBigIntValue(l - right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue(l - right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l - right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return this.subtract(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT subtract");
    }
  }

  @Override
  public Value multiply(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getBigIntValue(l * right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue(l * right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l * right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return this.multiply(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT multiply");
    }
  }

  @Override
  public Value divide(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Division by zero");
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getBigIntValue(l / right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue(l / right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l / right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return this.divide(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT divide");
    }
  }

  @Override
  public Value modulo(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Modulo by zero");
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getBigIntValue(l % right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue(l % right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue(l % right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return this.modulo(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT modulo");
    }
  }

  @Override
  public Value sqrt(Value val) {
    if (val.isNull()) return new Value(TypeId.DECIMAL); // NULL DECIMAL
    long v = val.asBigInt();
    if (v < 0) throw new DBException(ErrorType.INVALID_OPERATION, "Cannot sqrt negative bigint");
    return ValueFactory.getDecimalValue(Math.sqrt(v));
  }

  @Override
  public Value operateNull(Value left, Value right) {
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return new Value(TypeId.BIGINT);
      case DECIMAL:
        return new Value(TypeId.DECIMAL);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Type error in BIGINT operateNull");
    }
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l == right.asInt());
      case BIGINT:
        return CmpBool.from(l == right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l == right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return CmpBool.from(l == casted.asBigInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT compareEquals");
    }
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l != right.asInt());
      case BIGINT:
        return CmpBool.from(l != right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l != right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return CmpBool.from(l != casted.asBigInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT compareNotEquals");
    }
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l < right.asInt());
      case BIGINT:
        return CmpBool.from(l < right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l < right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return CmpBool.from(l < casted.asBigInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT compareLessThan");
    }
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l <= right.asInt());
      case BIGINT:
        return CmpBool.from(l <= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l <= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return CmpBool.from(l <= casted.asBigInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT compareLessThanEquals");
    }
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l > right.asInt());
      case BIGINT:
        return CmpBool.from(l > right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l > right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return CmpBool.from(l > casted.asBigInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT compareGreaterThan");
    }
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    long l = left.asBigInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l >= right.asInt());
      case BIGINT:
        return CmpBool.from(l >= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l >= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.BIGINT);
        return CmpBool.from(l >= casted.asBigInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in BIGINT compareGreaterThanEquals");
    }
  }

  // ---------- Misc ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "bigint_null";
    return String.valueOf(val.asBigInt());
  }

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    long v = val.asBigInt();
    ByteBuffer.wrap(storage, offset, Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(v);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    long v = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN).getLong();
    return ValueFactory.getBigIntValue(v);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.BIGINT);
    return new Value(TypeId.BIGINT, val.asBigInt());
  }

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    long v = val.asBigInt();
    switch (targetType) {
      case TINYINT:
        if (v > Limits.INT8_MAX || v < Limits.INT8_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for TINYINT");
        }
        return new Value(TypeId.TINYINT, (int) v);
      case SMALLINT:
        if (v > Limits.INT16_MAX || v < Limits.INT16_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for SMALLINT");
        }
        return new Value(TypeId.SMALLINT, (int) v);
      case INTEGER:
        if (v > Limits.INT32_MAX || v < Limits.INT32_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for INTEGER");
        }
        return new Value(TypeId.INTEGER, (int) v);
      case BIGINT:
        return new Value(TypeId.BIGINT, v);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, (double) v);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, String.valueOf(v));
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "BIGINT not coercible to " + targetType);
    }
  }

  @Override
  public byte[] getData(Value val) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(val.asBigInt());
    return buffer.array();
  }

  @Override
  public int getStorageSize(Value val) {
    return Long.BYTES; // 8
  }
}
