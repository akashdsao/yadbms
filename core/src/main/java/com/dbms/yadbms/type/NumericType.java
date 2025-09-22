package com.dbms.yadbms.type;

/**
 * NumericType - abstract class for numeric SQL types (integers, decimals, etc.). Provides abstract
 * arithmetic and comparison operations.
 */
public abstract class NumericType extends Type {

  protected NumericType(TypeId typeId) {
    super(typeId);
  }

  public abstract Value add(Value left, Value right);

  public abstract Value subtract(Value left, Value right);

  public abstract Value multiply(Value left, Value right);

  public abstract Value divide(Value left, Value right);

  public abstract Value modulo(Value left, Value right);

  // Math functions
  public abstract Value sqrt(Value val);

  // Null operation (how to behave if one operand is null)
  public abstract Value operateNull(Value left, Value right);

  // Zero-check
  public abstract boolean isZero(Value val);

  protected static double valMod(double x, double y) {
    return x - Math.floor(x / y) * y;
  }
}
