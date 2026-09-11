package com.aion.aion.error;

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
    super(null, String.format("Non-monotonic write: last=%d incoming=%d", lastT, incomingT), null);
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
  public Map<String, Object> context() {
    Map<String, Object> ctx = super.context();
    ctx.put("lastT", lastT);
    ctx.put("incomingT", incomingT);
    return ctx;
  }
}
