package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;

/** IntegerType - 32-bit signed integer SQL type. */
public class IntegerType extends IntegerParentType {

  public IntegerType() {
    super(TypeId.INTEGER);
  }

  @Override
  public boolean isZero(Value val) {
    return !val.isNull() && val.asInt() == 0;
  }

  @Override
  public Value add(Value left, Value right) {
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    if (!left.checkInteger() || !left.checkComparable(right)) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Invalid operands for add");
    }
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        {
          int r = right.asInt();
          long result = (long) l + (long) r; // widen to detect overflow
          if (result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "Integer addition overflow");
          }
          return ValueFactory.getIntegerValue((int) result);
        }
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l + right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l + right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return this.add(left, casted); // reuse
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in add");
    }
  }

  @Override
  public Value subtract(Value left, Value right) {
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        {
          int r = right.asInt();
          long result = (long) l - (long) r;
          if (result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "Integer subtraction overflow");
          }
          return ValueFactory.getIntegerValue((int) result);
        }
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l - right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l - right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return this.subtract(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in subtract");
    }
  }

  @Override
  public Value multiply(Value left, Value right) {
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        {
          int r = right.asInt();
          long result = (long) l * (long) r;
          if (result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
            throw new DBException(ErrorType.OUT_OF_RANGE, "Integer multiplication overflow");
          }
          return ValueFactory.getIntegerValue((int) result);
        }
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l * right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l * right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return this.multiply(left, casted);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in multiply");
    }
  }

  @Override
  public Value divide(Value left, Value right) {
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    if (isZero(right)) {
      throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Division by zero");
    }
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getIntegerValue(l / right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l / right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue((double) l / right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return ValueFactory.getIntegerValue(l / casted.asInt());
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in divide");
    }
  }

  @Override
  public Value modulo(Value left, Value right) {
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    if (isZero(right)) {
      throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Modulo by zero");
    }
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ValueFactory.getIntegerValue(l % right.asInt());
      case BIGINT:
        return ValueFactory.getBigIntValue((long) l % right.asBigInt());
      case DECIMAL:
        return ValueFactory.getDecimalValue(l % right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return ValueFactory.getIntegerValue(l % casted.asInt());
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in modulo");
    }
  }

  @Override
  public Value sqrt(Value val) {
    if (val.isNull()) {
      return operateNull(val, val);
    }
    int v = val.asInt();
    if (v < 0) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Cannot sqrt negative integer");
    }
    return ValueFactory.getDecimalValue(Math.sqrt(v));
  }

  @Override
  public Value operateNull(Value left, Value right) {
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return new Value(TypeId.INTEGER, Limits.INT32_NULL);
      case BIGINT:
        return new Value(TypeId.BIGINT, Limits.INT64_NULL);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, Limits.DECIMAL_NULL);
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Type error in operateNull");
    }
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l == right.asInt());
      case BIGINT:
        return CmpBool.from((long) l == right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l == right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return CmpBool.from(l == casted.asInt());
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in compareEquals");
    }
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l != right.asInt());
      case BIGINT:
        return CmpBool.from((long) l != right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l != right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return CmpBool.from(l != casted.asInt());
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in compareNotEquals");
    }
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l < right.asInt());
      case BIGINT:
        return CmpBool.from((long) l < right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l < right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return CmpBool.from(l < casted.asInt());
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in compareLessThan");
    }
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l <= right.asInt());
      case BIGINT:
        return CmpBool.from((long) l <= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l <= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return CmpBool.from(l <= casted.asInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in compareLessThanEquals");
    }
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l > right.asInt());
      case BIGINT:
        return CmpBool.from((long) l > right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l > right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return CmpBool.from(l > casted.asInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in compareGreaterThan");
    }
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    int l = left.asInt();
    switch (right.getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return CmpBool.from(l >= right.asInt());
      case BIGINT:
        return CmpBool.from((long) l >= right.asBigInt());
      case DECIMAL:
        return CmpBool.from((double) l >= right.asDecimal());
      case VARCHAR:
        Value casted = right.castAs(TypeId.INTEGER);
        return CmpBool.from(l >= casted.asInt());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in compareGreaterThanEquals");
    }
  }

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "integer_null";
    return String.valueOf(val.asInt());
  }

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    int v = val.asInt();
    ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(v);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    int v = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN).getInt();
    return ValueFactory.getIntegerValue(v);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.INTEGER);
    return new Value(TypeId.INTEGER, val.asInt());
  }

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    int v = val.asInt();
    switch (targetType) {
      case TINYINT:
        if (v > Limits.INT8_MAX || v < Limits.INT8_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for TINYINT");
        }
        return new Value(TypeId.TINYINT, v);
      case SMALLINT:
        if (v > Limits.INT16_MAX || v < Limits.INT16_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for SMALLINT");
        }
        return new Value(TypeId.SMALLINT, v);
      case INTEGER:
        return new Value(TypeId.INTEGER, v);
      case BIGINT:
        return new Value(TypeId.BIGINT, (long) v);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, (double) v);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, String.valueOf(v));
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "INTEGER not coercible to " + targetType);
    }
  }

  @Override
  public byte[] getData(Value val) throws NotImplementedException {
    throw new NotImplementedException("");
  }

  @Override
  public int getStorageSize(Value val) {
    return Integer.BYTES;
  }
}
