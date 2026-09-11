package com.aion.aion;

import com.aion.aion.error.AionEndOfStreamException;
import com.aion.aion.error.AionQueueReadException;

/**
 * Per-tick coordination strategy for an {@link AionMultiQueueReader}. Exactly one instance is bound
 * to each reader (selected by its {@link AionMultiQueueReader.Mode}) and owns whatever mode-specific
 * state and wake-signal lifecycle that coordination requires, keeping the reader itself
 * coordination-agnostic.
 *
 * <p>A strategy is confined to its reader's single owner thread, except for the wake signal a {@link
 * LatestAnyStrategy} shares with subscribed writers. Adding a new coordination mode is a new {@code
 * TickStrategy} implementation plus a constant in {@link AionMultiQueueReader.Mode} — the reader is
 * untouched.
 *
 * @param <T> item type
 */
interface TickStrategy<T> extends AutoCloseable {

  /**
   * Advance one coordinated step and return the timestamp it resolved to. {@code deadlineNanos ==
   * null} blocks indefinitely; otherwise wait until that absolute {@link System#nanoTime()} before
   * raising {@link AionQueueReadException}.
   */
  long tick(Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException;

  /** Release any cross-thread resources (e.g. a registered wake signal). Idempotent. */
  @Override
  default void close() {}
}
