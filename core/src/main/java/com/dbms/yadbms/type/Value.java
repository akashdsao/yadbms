package com.dbms.yadbms.type;

import static com.dbms.yadbms.type.TypeId.BOOLEAN;
import static com.dbms.yadbms.type.TypeId.VARCHAR;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.util.Arrays;
import lombok.Getter;

/**
 * A Value is a runtime representation of SQL data (scalar, string, vector). In java, we rely on GC,
 * so no manual memory management is needed.
 */
public class Value {
  @Getter private final TypeId typeId;
  private final Object value;
  private final boolean isNull;

  public Value(TypeId type) {
    this.typeId = type;
    this.value = null;
    this.isNull = true;
  }

  public Value(TypeId type, boolean b) {
    this.typeId = type;
    this.value = b;
    this.isNull = false;
  }

  public Value(TypeId type, byte i) {
    this.typeId = type;
    this.value = i;
    this.isNull = false;
  }

  public Value(TypeId type, short i) {
    this.typeId = type;
    this.value = i;
    this.isNull = false;
  }

  public Value(TypeId type, int i) {
    this.typeId = type;
    this.value = i;
    this.isNull = false;
  }

  public Value(TypeId type, long l) {
    this.typeId = type;
    this.value = l;
    this.isNull = false;
  }

  public Value(TypeId type, double d) {
    this.typeId = type;
    this.value = d;
    this.isNull = false;
  }

  public Value(TypeId type, String s) {
    if (type != VARCHAR) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Invalid type for String: " + type);
    }
    this.typeId = type;
    this.value = s;
    this.isNull = (s == null);
  }

  public Value(TypeId type, double[] vec) {
    if (type != TypeId.VECTOR) {
      throw new IllegalArgumentException("Invalid type for Vector: " + type);
    }
    this.typeId = type;
    this.value = (vec == null) ? null : Arrays.copyOf(vec, vec.length); // defensive copy
    this.isNull = (vec == null);
  }

  public boolean isNull() {
    return isNull;
  }

  public Boolean asBoolean() {
    return (Boolean) value;
  }

  public Integer asInt() {
    return (Integer) value;
  }

  public Short asSmallInt() {
    return (Short) value;
  }

  public Byte asTinyInt() {
    return (Byte) value;
  }

  public Long asBigInt() {
    return (Long) value;
  }

  public Double asDecimal() {
    return (Double) value;
  }

  public String asString() {
    return (String) value;
  }

  public double[] asVector() {
    return (value == null) ? null : Arrays.copyOf((double[]) value, ((double[]) value).length);
  }

  public int getStorageSize() {
    return TypeFactory.getInstance(getTypeId()).getStorageSize(this);
  }

  public CmpBool compareEquals(Value other) {
    return TypeFactory.getInstance(this.typeId).compareEquals(this, other);
  }

  public CmpBool compareNotEquals(Value other) {
    return TypeFactory.getInstance(this.typeId).compareNotEquals(this, other);
  }

  public CmpBool compareLessThan(Value other) {
    return TypeFactory.getInstance(this.typeId).compareLessThan(this, other);
  }

  public CmpBool compareLessThanEquals(Value other) {
    return TypeFactory.getInstance(this.typeId).compareLessThanEquals(this, other);
  }

  public CmpBool compareGreaterThan(Value other) {
    return TypeFactory.getInstance(this.typeId).compareGreaterThan(this, other);
  }

  public CmpBool compareGreaterThanEquals(Value other) {
    return TypeFactory.getInstance(this.typeId).compareGreaterThanEquals(this, other);
  }

  public Value operateNull(Value o) {
    return TypeFactory.getInstance(this.getTypeId()).operateNull(this, o);
  }

  @Override
  public String toString() {
    if (isNull) return "NULL";
    switch (typeId) {
      case BOOLEAN:
        return String.valueOf(asBoolean());
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return String.valueOf(asInt());
      case BIGINT:
      case TIMESTAMP:
        return String.valueOf(asBigInt());
      case DECIMAL:
        return String.valueOf(asDecimal());
      case VARCHAR:
        return "\"" + asString() + "\"";
      case VECTOR:
        return Arrays.toString(asVector());
      default:
        return "INVALID";
    }
  }

  public Value copy() {
    if (isNull) return new Value(typeId);
    switch (typeId) {
      case VARCHAR:
        return new Value(typeId, asString());
      case VECTOR:
        return new Value(typeId, asVector());
      default:
        return this; // immutable scalars can be shared
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getAs(Class<T> clazz) {
    if (isNull) {
      return null;
    }
    if (!clazz.isInstance(value)) {
      throw new ClassCastException(
          "Cannot cast stored value of type "
              + value.getClass().getSimpleName()
              + " to "
              + clazz.getSimpleName());
    }
    return (T) value;
  }

  public Value castAs(TypeId targetType) {
    return TypeFactory.getInstance(this.getTypeId()).castAs(this, targetType);
  }

  public void serializeTo(byte[] storage, int offset) {
    TypeFactory.getInstance(this.getTypeId()).serializeTo(this, storage, offset);
  }

  public static Value deserializeFrom(byte[] storage,TypeId type){
    return TypeFactory.getInstance(type).deserializeFrom(storage);
  }

  public boolean checkInteger() {
    switch (getTypeId()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return true;
      default:
        break;
    }
    return false;
  }

  public boolean checkComparable(Value o) {
    switch (getTypeId()) {
      case BOOLEAN:
        return (o.getTypeId() == BOOLEAN || o.getTypeId() == VARCHAR);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
        switch (o.getTypeId()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case DECIMAL:
          case VARCHAR:
            return true;
          default:
            break;
        }
        break;
      case VARCHAR:
        // Anything can be cast to a string!
        return true;
      default:
        break;
    }
    return false;
  }
}
