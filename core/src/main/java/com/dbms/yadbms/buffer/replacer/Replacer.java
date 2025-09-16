package com.dbms.yadbms.buffer.replacer;

import com.dbms.yadbms.config.FrameId;
import java.util.Optional;

/**
 * Replacer interface for managing frame replacement in a buffer. It is an interface that tracks the
 * frame usage.
 */
public interface Replacer {

  /**
   * Attempts to select a frame to be replaced. If a frame is available, it returns an Optional
   * containing the FrameId of the victim frame. If no frame is available, it returns an empty
   * Optional.
   *
   * @return an Optional containing the FrameId of the victim frame, or empty if no frame is
   *     available
   */
  public Optional<FrameId> victim();

  /**
   * Pins a frame in the buffer, preventing it from being replaced.
   *
   * @param frameId the FrameId of the frame to be pinned
   */
  public void pin(FrameId frameId);

  /**
   * Unpins a frame in the buffer, allowing it to be replaced.
   *
   * @param frameId the FrameId of the frame to be unpinned
   */
  public void unPin(FrameId frameId);

  /**
   * Records access to a frame, updating its usage status. This method is typically called when a
   * frame is accessed, to keep track of its usage.
   *
   * @param frameId the FrameId of the frame that was accessed
   */
  public void recordAccess(FrameId frameId);

  /**
   * Number of elements in the buffer.
   *
   * @return the number of elements in the replacer that can be victimized
   */
  public int size();
}
