package com.aion.error.queue;

import com.aion.error.ErrorCode;
import java.util.HashMap;
import java.util.Map;

/**
 * Thrown by {@code AionQueueWriter.write} when an incoming timestamp is not strictly greater than
 * the last written timestamp. One writer per queue is assumed with strictly monotonic timestamps; a
 * regression is a contract violation.
 */
public final class AionQueueMonotonicityException extends AionQueueException {

  private final long lastT;
  private final long incomingT;

  public AionQueueMonotonicityException(long lastT, long incomingT) {
    super(
        ErrorCode.QUEUE_MONOTONICITY_VIOLATION,
        null,
        String.format("Non-monotonic write: last=%d incoming=%d", lastT, incomingT),
        null);
    this.lastT = lastT;
    this.incomingT = incomingT;
  }

  public long getLastT() {
    return lastT;
  }

  public long getIncomingT() {
    return incomingT;
  }

  @Override
  protected Map<String, Object> contextMap() {
    Map<String, Object> ctx = new HashMap<>(super.contextMap());
    ctx.put("lastT", lastT);
    ctx.put("incomingT", incomingT);
    return ctx;
  }
}
