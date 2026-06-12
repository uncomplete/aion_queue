package com.aion.error.queue;

import com.aion.error.ErrorCode;

/**
 * Thrown by {@code AionQueueReader.tick()} / {@code tickTo()} (and by {@code
 * AionMultiQueueReader.tick()}) when the writer has closed and no remaining buffered item satisfies
 * the requested target.
 */
public final class AionEndOfStreamException extends AionQueueException {

  public AionEndOfStreamException(String message) {
    super(ErrorCode.QUEUE_END_OF_STREAM, null, message, null);
  }

  public AionEndOfStreamException() {
    this("End of stream");
  }
}
