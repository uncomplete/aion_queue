package com.aion.aion.error;

import java.util.HashMap;
import java.util.Map;

/**
 * Base checked exception for all {@link com.aion.aion.AionQueue} / {@link com.aion.aion.AionMultiQueue} errors. Carries an
 * optional stream id plus subclass-specific typed fields exposed via {@link #context()} so a host
 * application can translate queue failures into its own error model.
 */
public class AionQueueException extends Exception {

  private final String streamId;

  public AionQueueException(String streamId, String message, Throwable cause) {
    super(message, cause);
    this.streamId = streamId;
  }

  /** The stream this failure relates to, or {@code null} when not stream-specific. */
  public String getStreamId() {
    return streamId;
  }

  /** Structured view of this exception's typed fields, for logging or translation by callers. */
  public Map<String, Object> context() {
    Map<String, Object> ctx = new HashMap<>();
    if (streamId != null) ctx.put("streamId", streamId);
    return ctx;
  }
}
