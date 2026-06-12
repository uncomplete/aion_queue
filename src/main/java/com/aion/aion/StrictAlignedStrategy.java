package com.aion.aion;

import com.aion.error.queue.AionEndOfStreamException;
import com.aion.error.queue.AionQueueReadException;
import java.util.Map;

/**
 * {@link AionMultiQueueReader.Mode#STRICT_ALIGNED} coordination: every tick returns the smallest
 * timestamp at which every subscribed stream advances together. Sub-readers that lack that exact t
 * advance to the smallest item at or above it (skipping intermediate items they uniquely had).
 *
 * <p>Stateless apart from the immutable sub-reader map.
 *
 * @param <T> item type
 */
final class StrictAlignedStrategy<T> implements TickStrategy<T> {

  private final Map<String, AionQueueReader<T>> readers;

  StrictAlignedStrategy(Map<String, AionQueueReader<T>> readers) {
    this.readers = readers;
  }

  @Override
  public long tick(Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    long target = Long.MIN_VALUE;
    for (AionQueueReader<T> r : readers.values()) {
      long t = r.tickToInternal(Long.MIN_VALUE, deadlineNanos);
      if (t > target) target = t;
    }
    while (true) {
      boolean converged = true;
      for (AionQueueReader<T> r : readers.values()) {
        Long cur = r.currentT();
        long curT = cur == null ? Long.MIN_VALUE : cur;
        if (curT < target) {
          long t = r.tickToInternal(target, deadlineNanos);
          if (t > target) {
            target = t;
            converged = false;
          }
        }
      }
      if (converged) return target;
    }
  }
}
