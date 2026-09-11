package com.aion.aion.error;

/**
 * Thrown by the timed overloads of {@code AionQueueReader.tick/tickTo/peekNextT} (and {@code
 * AionMultiQueueReader.tick}) when the supplied timeout elapses before a satisfying item is
 * available.
 */
public final class AionQueueReadException extends AionQueueException {

  public AionQueueReadException(String message) {
    super(null, message, null);
  }
}
