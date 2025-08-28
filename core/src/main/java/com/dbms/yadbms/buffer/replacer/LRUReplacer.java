package com.dbms.yadbms.buffer.replacer;

import com.dbms.yadbms.config.FrameId;
import java.util.*;

public class LRUReplacer implements Replacer {

  private final int numPages;
  private final LinkedHashMap<FrameId, Boolean> lruMap;
  private final Set<FrameId> pinnedFrames;

  public LRUReplacer(int numPages) {
    this.numPages = numPages;
    this.lruMap = new LinkedHashMap<>(numPages, 0.75f, true);
    this.pinnedFrames = new HashSet<>();
  }

  @Override
  public Optional<FrameId> victim() {
    for (Iterator<Map.Entry<FrameId, Boolean>> it = lruMap.entrySet().iterator(); it.hasNext(); ) {
      FrameId frameId = it.next().getKey();
      if (!pinnedFrames.contains(frameId)) {
        it.remove();
        return Optional.of(frameId);
      }
    }
    return Optional.empty();
  }

  @Override
  public void pin(FrameId frameId) {
    pinnedFrames.add(frameId);
  }

  @Override
  public void unPin(FrameId frameId) {
    pinnedFrames.remove(frameId);
  }

  @Override
  public void recordAccess(FrameId frameId) {
    lruMap.put(frameId, true);
    while (size() > numPages) {
      victim();
    }
  }

  @Override
  public int size() {
    int count = 0;
    for (FrameId id : lruMap.keySet()) {
      if (!pinnedFrames.contains(id)) {
        count++;
      }
    }
    return count;
  }
}
