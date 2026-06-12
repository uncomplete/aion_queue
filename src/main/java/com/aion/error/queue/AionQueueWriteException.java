package com.aion.error.queue;

import com.aion.error.ErrorCode;
import java.util.HashMap;
import java.util.Map;

/**
 * Thrown by the timed overload of {@code AionQueueWriter.write} when the supplied timeout elapses
 * with the queue still at capacity (slowest reader has not advanced enough to free buffer space).
 */
public final class AionQueueWriteException extends AionQueueException {

  private final int bufferSize;
  private final int capacity;

  public AionQueueWriteException(int bufferSize, int capacity) {
    super(
        ErrorCode.QUEUE_WRITE,
        null,
        String.format(
            "Queue backpressure timeout: bufferSize=%d capacity=%d", bufferSize, capacity),
        null);
    this.bufferSize = bufferSize;
    this.capacity = capacity;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getCapacity() {
    return capacity;
  }

  @Override
  protected Map<String, Object> contextMap() {
    Map<String, Object> ctx = new HashMap<>(super.contextMap());
    ctx.put("bufferSize", bufferSize);
    ctx.put("capacity", capacity);
    return ctx;
  }
}
