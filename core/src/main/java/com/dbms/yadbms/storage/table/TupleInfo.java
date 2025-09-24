package com.dbms.yadbms.storage.table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/** Contains data: (offset, size, meta) */
@Getter
@Setter
@AllArgsConstructor
public class TupleInfo {
  int offset;
  int size;
  TupleMetaData tupleMetaData;
}
