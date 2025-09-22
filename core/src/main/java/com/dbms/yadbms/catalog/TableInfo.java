package com.dbms.yadbms.catalog;

import lombok.Data;

/** The TableInfo class maintains metadata about a table. */
@Data
public class TableInfo {
  private Schema schema;
  private String name;
}
