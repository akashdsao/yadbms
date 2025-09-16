package com.dbms.yadbms.storage.page;

import lombok.Getter;
import lombok.Setter;

public abstract class BPlusTreePage {
  @Setter private IndexPageType pageType = IndexPageType.INVALID_INDEX_PAGE;

  @Getter @Setter private int size;

  @Getter @Setter private int maxSize;

  public boolean isLeafPage() {
    return pageType == IndexPageType.LEAF_PAGE;
  }

  public void changeSizeBy(int amount) {
    size += amount;
  }

  public int getMinSize() {
    return (int) Math.ceil(maxSize / 2.0);
  }
}
