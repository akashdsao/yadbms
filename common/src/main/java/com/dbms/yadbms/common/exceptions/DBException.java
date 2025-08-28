package com.dbms.yadbms.common.exceptions;

public class DBException extends RuntimeException {
  private final String errorCode;

  public DBException(ErrorType errorType) {
    super(errorType.getDefaultMessage());
    this.errorCode = errorType.getCode();
  }

  public DBException(ErrorType errorType, Throwable cause) {
    super(errorType.getDefaultMessage(), cause);
    this.errorCode = errorType.getCode();
  }

  public DBException(ErrorType errorType, String customMessage) {
    super(customMessage);
    this.errorCode = errorType.getCode();
  }

  public DBException(ErrorType errorType, String customMessage, Throwable cause) {
    super(customMessage, cause);
    this.errorCode = errorType.getCode();
  }

  public String getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return "DBException{"
        + "errorCode='"
        + errorCode
        + '\''
        + ", message='"
        + getMessage()
        + '\''
        + '}';
  }
}
