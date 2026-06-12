package com.aion.error.queue;

import com.aion.error.AionException;
import com.aion.error.ErrorCode;
import java.util.HashMap;
import java.util.Map;

/**
 * Base exception for all {@link com.aion.aion.AionQueue} / {@link com.aion.aion.AionMultiQueue}
 * errors.
 */
public class AionQueueException extends AionException {

  private final String streamId;

  public AionQueueException(ErrorCode code, String streamId, String message, Throwable cause) {
    super(code, message, cause);
    this.streamId = streamId;
  }

  public String getStreamId() {
    return streamId;
  }

  @Override
  protected Map<String, Object> contextMap() {
    Map<String, Object> ctx = new HashMap<>();
    if (streamId != null) ctx.put("streamId", streamId);
    return ctx;
  }
}
