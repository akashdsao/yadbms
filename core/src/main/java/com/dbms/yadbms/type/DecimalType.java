package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** DecimalType - SQL DECIMAL type backed by Java double. */
public class DecimalType extends NumericType {

  public DecimalType() {
    super(TypeId.DECIMAL);
  }

  @Override
  public boolean isZero(Value val) {
    return !val.isNull() && val.asDecimal() == 0.0;
  }

  @Override
  public boolean isInlined(Value val) {
    return true;
  }

  // ---------- Arithmetic ----------

  @Override
  public Value add(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.DECIMAL, l + right.asTinyInt());
      case SMALLINT:
        return new Value(TypeId.DECIMAL, l + right.asSmallInt());
      case INTEGER:
        return new Value(TypeId.DECIMAL, l + right.asInt());
      case BIGINT:
        return new Value(TypeId.DECIMAL, l + right.asBigInt());
      case DECIMAL:
        return new Value(TypeId.DECIMAL, l + right.asDecimal());
      case VARCHAR:
        return this.add(left, right.castAs(TypeId.DECIMAL));
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL add");
    }
  }

  @Override
  public Value subtract(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.DECIMAL, l - right.asTinyInt());
      case SMALLINT:
        return new Value(TypeId.DECIMAL, l - right.asSmallInt());
      case INTEGER:
        return new Value(TypeId.DECIMAL, l - right.asInt());
      case BIGINT:
        return new Value(TypeId.DECIMAL, l - right.asBigInt());
      case DECIMAL:
        return new Value(TypeId.DECIMAL, l - right.asDecimal());
      case VARCHAR:
        return this.subtract(left, right.castAs(TypeId.DECIMAL));
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL subtract");
    }
  }

  @Override
  public Value multiply(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.DECIMAL, l * right.asTinyInt());
      case SMALLINT:
        return new Value(TypeId.DECIMAL, l * right.asSmallInt());
      case INTEGER:
        return new Value(TypeId.DECIMAL, l * right.asInt());
      case BIGINT:
        return new Value(TypeId.DECIMAL, l * right.asBigInt());
      case DECIMAL:
        return new Value(TypeId.DECIMAL, l * right.asDecimal());
      case VARCHAR:
        return this.multiply(left, right.castAs(TypeId.DECIMAL));
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL multiply");
    }
  }

  @Override
  public Value divide(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Division by zero");
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.DECIMAL, l / right.asTinyInt());
      case SMALLINT:
        return new Value(TypeId.DECIMAL, l / right.asSmallInt());
      case INTEGER:
        return new Value(TypeId.DECIMAL, l / right.asInt());
      case BIGINT:
        return new Value(TypeId.DECIMAL, l / right.asBigInt());
      case DECIMAL:
        return new Value(TypeId.DECIMAL, l / right.asDecimal());
      case VARCHAR:
        return this.divide(left, right.castAs(TypeId.DECIMAL));
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL divide");
    }
  }

  @Override
  public Value modulo(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    if (isZero(right)) throw new DBException(ErrorType.DIVIDE_BY_ZERO, "Modulo by zero");

    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return new Value(TypeId.DECIMAL, valMod(l, right.asTinyInt()));
      case SMALLINT:
        return new Value(TypeId.DECIMAL, valMod(l, right.asSmallInt()));
      case INTEGER:
        return new Value(TypeId.DECIMAL, valMod(l, right.asInt()));
      case BIGINT:
        return new Value(TypeId.DECIMAL, valMod(l, right.asBigInt()));
      case DECIMAL:
        return new Value(TypeId.DECIMAL, valMod(l, right.asDecimal()));
      case VARCHAR:
        return new Value(TypeId.DECIMAL, valMod(l, right.castAs(TypeId.DECIMAL).asDecimal()));
      default:
        throw new DBException(ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL modulo");
    }
  }

  @Override
  public Value min(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    return (left.compareLessThanEquals(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  @Override
  public Value max(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    return (left.compareGreaterThanEquals(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  @Override
  public Value sqrt(Value val) {
    if (val.isNull()) return new Value(TypeId.DECIMAL, Limits.DECIMAL_NULL);
    double v = val.asDecimal();
    if (v < 0) throw new DBException(ErrorType.INVALID_OPERATION, "Cannot sqrt negative DECIMAL");
    return new Value(TypeId.DECIMAL, Math.sqrt(v));
  }

  @Override
  public Value operateNull(Value left, Value right) {
    return new Value(TypeId.DECIMAL, Limits.DECIMAL_NULL);
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l == right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l == right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l == right.asInt());
      case BIGINT:
        return CmpBool.from(l == right.asBigInt());
      case DECIMAL:
        return CmpBool.from(l == right.asDecimal());
      case VARCHAR:
        return CmpBool.from(l == right.castAs(TypeId.DECIMAL).asDecimal());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL compareEquals");
    }
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (compareEquals(left, right) == CmpBool.CmpTrue) ? CmpBool.CmpFalse : CmpBool.CmpTrue;
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l < right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l < right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l < right.asInt());
      case BIGINT:
        return CmpBool.from(l < right.asBigInt());
      case DECIMAL:
        return CmpBool.from(l < right.asDecimal());
      case VARCHAR:
        return CmpBool.from(l < right.castAs(TypeId.DECIMAL).asDecimal());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL compareLessThan");
    }
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l <= right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l <= right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l <= right.asInt());
      case BIGINT:
        return CmpBool.from(l <= right.asBigInt());
      case DECIMAL:
        return CmpBool.from(l <= right.asDecimal());
      case VARCHAR:
        return CmpBool.from(l <= right.castAs(TypeId.DECIMAL).asDecimal());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL compareLessThanEquals");
    }
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l > right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l > right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l > right.asInt());
      case BIGINT:
        return CmpBool.from(l > right.asBigInt());
      case DECIMAL:
        return CmpBool.from(l > right.asDecimal());
      case VARCHAR:
        return CmpBool.from(l > right.castAs(TypeId.DECIMAL).asDecimal());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL compareGreaterThan");
    }
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    double l = left.asDecimal();
    switch (right.getTypeId()) {
      case TINYINT:
        return CmpBool.from(l >= right.asTinyInt());
      case SMALLINT:
        return CmpBool.from(l >= right.asSmallInt());
      case INTEGER:
        return CmpBool.from(l >= right.asInt());
      case BIGINT:
        return CmpBool.from(l >= right.asBigInt());
      case DECIMAL:
        return CmpBool.from(l >= right.asDecimal());
      case VARCHAR:
        return CmpBool.from(l >= right.castAs(TypeId.DECIMAL).asDecimal());
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "Unsupported type in DECIMAL compareGreaterThanEquals");
    }
  }

  // ---------- Cast ----------

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    double v = val.asDecimal();
    switch (targetType) {
      case TINYINT:
        if (v > Limits.INT8_MAX || v < Limits.INT8_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for TINYINT");
        }
        return new Value(TypeId.TINYINT, (byte) v);
      case SMALLINT:
        if (v > Limits.INT16_MAX || v < Limits.INT16_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for SMALLINT");
        }
        return new Value(TypeId.SMALLINT, (short) v);
      case INTEGER:
        if (v > Limits.INT32_MAX || v < Limits.INT32_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for INTEGER");
        }
        return new Value(TypeId.INTEGER, (int) v);
      case BIGINT:
        if (v > Limits.INT64_MAX || v < Limits.INT64_MIN) {
          throw new DBException(ErrorType.OUT_OF_RANGE, "Value out of range for BIGINT");
        }
        return new Value(TypeId.BIGINT, (long) v);
      case DECIMAL:
        return new Value(TypeId.DECIMAL, v);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, String.valueOf(v));
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "DECIMAL not coercible to " + targetType);
    }
  }

  // ---------- Misc ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "decimal_null";
    return String.valueOf(val.asDecimal());
  }

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    double v = val.asDecimal();
    ByteBuffer.wrap(storage, offset, Double.BYTES).order(ByteOrder.BIG_ENDIAN).putDouble(v);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    double v = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN).getDouble();
    return new Value(TypeId.DECIMAL, v);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.DECIMAL);
    return new Value(TypeId.DECIMAL, val.asDecimal());
  }

  @Override
  public byte[] getData(Value val) {
    ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN);
    buffer.putDouble(val.asDecimal());
    return buffer.array();
  }

  @Override
  public int getStorageSize(Value val) {
    return Double.BYTES; // 8
  }
}
