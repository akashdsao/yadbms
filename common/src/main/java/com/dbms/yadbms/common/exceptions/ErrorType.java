package com.dbms.yadbms.common.exceptions;

public enum ErrorType {
  INVALID_ARGUMENT("INVALID_ARGUMENT", "The argument provided is invalid."),
  RESOURCE_NOT_FOUND("RESOURCE_NOT_FOUND", "Requested resource could not be found."),
  INTERNAL_ERROR("INTERNAL_ERROR", "An internal error occurred."),
  IO_ERROR("IO_ERROR", "I/O operation failed."),
  CONFIGURATION_ERROR("CONFIGURATION_ERROR", "Configuration is missing or invalid."),
  TIMEOUT("TIMEOUT", "The operation timed out."),
  PERMISSION_DENIED("PERMISSION_DENIED", "You do not have permission to perform this action."),
  INVALID_FRAME_ID("INVALID_FRAME_ID", "The provided frame ID is invalid."),
  INVALID_OPERATION("INVALID_OPERATION", "The operation is not valid in the current context."),
  EVICTION_FAILED_ERROR("EVICTION_FAILED_ERROR", "Failed to evict the frame from the buffer pool."),
  INVALID_PAGE_ID("INVALID_PAGE_ID", "The provided page ID is invalid."),
  INDEX_NOT_FOUND("INDEX_NOT_FOUND", "The specified index was not found."),
  OUT_OF_RANGE("OUT_OF_RANGE", "value overflown"),
  DIVIDE_BY_ZERO("DIVIDE_BY_ZERO", "Division by zero"),
  UNSUPPORTED_OPERATION("UNSUPPORTED_OPERATION", "Operation unsupported");

  private final String code;
  private final String defaultMessage;

  ErrorType(String code, String defaultMessage) {
    this.code = code;
    this.defaultMessage = defaultMessage;
  }

  public String getCode() {
    return code;
  }

  public String getDefaultMessage() {
    return defaultMessage;
  }
}
