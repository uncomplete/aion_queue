package com.aion.aion.error;

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
  public Map<String, Object> context() {
    Map<String, Object> ctx = super.context();
    ctx.put("bufferSize", bufferSize);
    ctx.put("capacity", capacity);
    return ctx;
  }
}
