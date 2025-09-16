package com.dbms.yadbms.buffer.replacer;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.FrameId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// TODO: This needs re-write
public class LRUKReplacer {

  private final Map<FrameId, LRUKNode> nodes;
  private final int replacerSize;
  private int currentTimestamp = 0;
  private int currentSize = 0;

  public LRUKReplacer(int replacerSize) {
    this.replacerSize = replacerSize;
    nodes = new HashMap<>();
  }

  /**
   * Attempts to select a frame to be replaced. Implements LRUk (Least Recently Used K) replacement
   * policy. In case of a tie, it selects the oldest frame. If no frame is available, it returns an
   * empty Optional.
   *
   * @return an Optional containing the FrameId of the victim frame, or empty if no frame is
   *     available
   */
  public synchronized Optional<FrameId> evict() {
    Optional<FrameId> victimNode = Optional.empty();
    int maxDistance = 0;
    int oldestTimestamp = Integer.MAX_VALUE;
    for (Map.Entry<FrameId, LRUKNode> entry : nodes.entrySet()) {
      FrameId fId = entry.getKey();
      LRUKNode node = entry.getValue();

      int distance = node.getKthDistance(currentTimestamp);
      int old = node.getOldestAccess();
      if (node.isEvictable()
          && (victimNode.isPresent()
              || distance > maxDistance
              || (distance == maxDistance && old < oldestTimestamp))) {
        maxDistance = distance;
        victimNode = Optional.of(fId);
        oldestTimestamp = old;
      }
    }
    if (victimNode.isPresent()) {
      nodes.remove(victimNode.get());
      currentSize--;
    }
    return victimNode;
  }

  /**
   * Pins/Unpins a frame in the buffer, preventing it from being replaced.
   *
   * @param frameId the FrameId of the frame to be pinned
   */
  public synchronized void setEvictable(FrameId frameId, boolean evictable) {
    if (!nodes.containsKey(frameId)) {
      throw new DBException(ErrorType.INVALID_FRAME_ID, "FrameId does not exist: " + frameId);
    }
    if (nodes.get(frameId).isEvictable() != evictable) {
      nodes.get(frameId).setEvictable(evictable);
      currentSize += evictable ? 1 : -1;
    }
  }

  /**
   * Removes a frame from the replacer. This method should only be called for frames that are
   * evictable.
   *
   * @param frameId the FrameId of the frame to be removed
   */
  public synchronized void removeFrame(FrameId frameId) {
    if (!nodes.containsKey(frameId)) {
      throw new DBException(ErrorType.INVALID_FRAME_ID, "FrameId does not exist: " + frameId);
    }
    if (!nodes.get(frameId).isEvictable()) {
      throw new DBException(
          ErrorType.INVALID_OPERATION, "Cannot remove a pinned frame: " + frameId);
    }
    nodes.remove(frameId);
  }

  /**
   * Records access to a frame, updating its usage status. This method is typically called when a
   * frame is accessed, to keep track of its usage.
   *
   * @param frameId the FrameId of the frame that was accessed
   */
  public synchronized void recordAccess(FrameId frameId) {
    currentTimestamp++;
    if (!nodes.containsKey(frameId)) {
      nodes.put(frameId, LRUKNode.builder().frameId(frameId).build());
    }
    nodes.get(frameId).recordAccess(currentTimestamp);
    currentSize--;
  }

  /**
   * Returns the number of elements in the replacer that can be victimized.
   *
   * @return the number of elements in the replacer
   */
  public synchronized int size() {
    return currentSize;
  }
}
