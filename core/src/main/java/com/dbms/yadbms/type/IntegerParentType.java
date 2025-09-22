package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;

/** All integer types to inherit this class. */
public abstract class IntegerParentType extends NumericType {

  protected IntegerParentType(TypeId typeId) {
    super(typeId);
  }

  @Override
  public abstract Value add(Value left, Value right);

  @Override
  public abstract Value subtract(Value left, Value right);

  @Override
  public abstract Value multiply(Value left, Value right);

  @Override
  public abstract Value divide(Value left, Value right);

  @Override
  public abstract Value modulo(Value left, Value right);

  @Override
  public abstract Value sqrt(Value val);

  @Override
  public abstract CmpBool compareEquals(Value left, Value right);

  @Override
  public abstract CmpBool compareNotEquals(Value left, Value right);

  @Override
  public abstract CmpBool compareLessThan(Value left, Value right);

  @Override
  public abstract CmpBool compareLessThanEquals(Value left, Value right);

  @Override
  public abstract CmpBool compareGreaterThan(Value left, Value right);

  @Override
  public abstract CmpBool compareGreaterThanEquals(Value left, Value right);

  @Override
  public abstract Value castAs(Value val, TypeId targetType);

  @Override
  public boolean isInlined(Value val) {
    return true;
  }

  @Override
  public abstract String toString(Value val);

  @Override
  public abstract void serializeTo(Value val, byte[] storage, int offset);

  @Override
  public abstract Value deserializeFrom(byte[] storage);

  @Override
  public abstract Value copy(Value val);

  @Override
  public abstract Value operateNull(Value left, Value right);

  @Override
  public abstract boolean isZero(Value val);

  @Override
  public Value min(Value left, Value right) {
    if (!left.checkInteger() || !left.checkComparable(right)) {
      throw new DBException(
          ErrorType.INVALID_OPERATION,
          "Min: Values are not comparable integer types ("
              + left.getTypeId()
              + ", "
              + right.getTypeId()
              + ")");
    }
    if (left.isNull() || right.isNull()) {
      return operateNull(left, right);
    }
    return (left.compareLessThan(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  @Override
  public Value max(Value left, Value right) {
    if (!left.checkInteger() || !left.checkComparable(right)) {
      throw new DBException(
          ErrorType.INVALID_OPERATION,
          "Max: Values are not comparable integer types ("
              + left.getTypeId()
              + ", "
              + right.getTypeId()
              + ")");
    }
    if (left.isNull() || right.isNull()) {
      return left.operateNull(right);
    }
    return (left.compareGreaterThanEquals(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }
}
