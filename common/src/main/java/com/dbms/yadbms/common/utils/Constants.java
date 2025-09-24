package com.dbms.yadbms.common.utils;

public final class Constants {
  public static final int DEFAULT_FRAME_ID = -1;
  public static final int INVALID_PAGE_ID = -1;

  /** starting size of file on disk */
  public static final long DEFAULT_DB_IO_SIZE = 16;

  /** size of data page in byte */
  public static final int PAGE_SIZE = 4096; // 4kB

  public static final int LRU_REPLACER_K = 10;

  public static final int INTERNAL_PAGE_SIZE = 32;

  public static final int TABLE_PAGE_HEADER_SIZE = 24; // can adjust as needed
  public static final int TUPLE_INFO_SIZE = 16 + 4 + 4; // offset(4) + size(4) + meta(16)
}
