package com.dbms.yadbms.catalog;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.type.TypeId;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

public class Column {
  @NonNull @Getter private String columnName;
  @NonNull @Getter private final TypeId columnType;
  private int length;
  @Getter @Setter private int columnOffset;

  public Column(String columnName, TypeId type) {
    if (type == TypeId.VARCHAR || type == TypeId.VECTOR) {
      throw new DBException(ErrorType.INVALID_OPERATION, "Wrong constructor for fixed-size type.");
    }
    this.columnName = columnName;
    this.columnType = type;
    this.length = typeSize(type, 0);
  }

  public Column(String columnName, TypeId type, int length) {
    if (type != TypeId.VARCHAR && type != TypeId.VECTOR) {
      throw new DBException(
          ErrorType.INVALID_OPERATION, "Wrong constructor for variable-size type");
    }
    this.columnName = columnName;
    this.columnType = type;
    this.length = typeSize(type, length);
  }

  private Column(String columnName, Column other) {
    this.columnName = columnName;
    this.columnType = other.columnType;
    this.length = other.length;
    this.columnOffset = other.columnOffset;
  }

  /** Return a copy of this column with a different name. */
  public Column withColumnName(String newName) {
    return new Column(newName, this);
  }

  public int getStorageSize() {
    return length;
  }

  public int getOffset() {
    return columnOffset;
  }

  public boolean isInlined() {
    return columnType != TypeId.VARCHAR && columnType != TypeId.VECTOR;
  }

  public String toString(boolean simplified) {
    if (simplified) {
      StringBuilder sb = new StringBuilder();
      sb.append(columnName).append(":").append(columnType);

      if (columnType == TypeId.VARCHAR) {
        sb.append("(").append(length).append(")");
      }
      if (columnType == TypeId.VECTOR) {
        sb.append("(").append(length / Double.BYTES).append(")");
      }
      return sb.toString();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Column[")
        .append(columnName)
        .append(", ")
        .append(columnType)
        .append(", ")
        .append("Offset:")
        .append(columnOffset)
        .append(", ")
        .append("Length:")
        .append(length)
        .append("]");
    return sb.toString();
  }

  private static int typeSize(TypeId type, int length) {
    switch (type) {
      case BOOLEAN:
      case TINYINT:
        return 1;
      case SMALLINT:
        return 2;
      case INTEGER:
        return 4;
      case BIGINT:
      case DECIMAL:
      case TIMESTAMP:
        return 8;
      case VARCHAR:
        return length;
      case VECTOR:
        return length * Double.BYTES;
      default:
        throw new IllegalArgumentException("Cannot get size of invalid type: " + type);
    }
  }
}
