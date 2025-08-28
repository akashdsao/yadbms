package com.dbms.yadbms.buffer.replacer;

import com.dbms.yadbms.config.FrameId;
import java.util.Deque;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class LRUKNode {
  private FrameId frameId;
  private int k;
  private Deque<Integer> history;
  private boolean isEvictable = true;

  public void recordAccess(int timestamp) {
    history.addLast(timestamp);
    if (history.size() > k) {
      history.removeFirst();
    }
  }

  public int getKthDistance(int currentTimestamp) {
    if (history.size() < k) {
      return Integer.MAX_VALUE;
    }
    return currentTimestamp - history.getLast();
  }

  public int getOldestAccess() {
    return history.getFirst();
  }

  public boolean isEvictable() {
    return isEvictable;
  }

  public void setEvictable(boolean evictable) {
    isEvictable = evictable;
  }
}
