package com.dbms.yadbms.storage.page;

import static com.dbms.yadbms.common.utils.Constants.INVALID_PAGE_ID;

import com.dbms.yadbms.config.PageId;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Setter
@Getter
public class RecordId {
  private PageId pageId = PageId.store(INVALID_PAGE_ID);
  private int slotNumber = 0;

  public  RecordId(PageId pageId, int slotNumber){
    this.pageId = pageId;
    this.slotNumber = slotNumber;
  }

  @Override
  public String toString() {
    return "RecordId{" + "pageId=" + pageId + ", slotNumber=" + slotNumber + '}';
  }
}
