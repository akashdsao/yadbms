package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.util.EnumMap;
import java.util.Map;

/** TypeFactory, holds singleton instances for all Type implementations. */
public final class TypeFactory {
  private static final Map<TypeId, Type> types = new EnumMap<>(TypeId.class);

  static {
    types.put(TypeId.BOOLEAN, new BooleanType());
    types.put(TypeId.INTEGER, new IntegerType());
    types.put(TypeId.BIGINT, new BigIntType());
    types.put(TypeId.SMALLINT, new SmallIntType());
    types.put(TypeId.TINYINT, new TinyIntType());
    types.put(TypeId.DECIMAL, new DecimalType());
    types.put(TypeId.VARCHAR, new VarCharType());
    types.put(TypeId.VECTOR, new VectorType());
  }

  private TypeFactory() {
    throw new DBException(ErrorType.INVALID_OPERATION, "Unsustainable factory class used");
  }

  public static Type getInstance(TypeId typeId) {
    Type t = types.get(typeId);
    if (t == null) {
      throw new DBException(
          ErrorType.INVALID_OPERATION, "No type registered for type " + typeId.name());
    }
    return t;
  }
}
