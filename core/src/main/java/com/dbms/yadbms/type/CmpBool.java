package com.dbms.yadbms.type;

public enum CmpBool {
  CmpFalse,
  CmpTrue,
  CmpNull;

  /** Convert a Java boolean into a CmpBool (ignores NULLs). */
  public static CmpBool from(boolean b) {
    return b ? CmpTrue : CmpFalse;
  }
}
