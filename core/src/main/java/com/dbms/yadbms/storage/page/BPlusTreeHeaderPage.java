package com.dbms.yadbms.storage.page;

import com.dbms.yadbms.config.PageId;
import lombok.Getter;
import lombok.Setter;

public class BPlusTreeHeaderPage {
  @Getter @Setter private PageId rootPageId;
}
