package com.aion.aion;

import com.aion.error.queue.AionEndOfStreamException;
import com.aion.error.queue.AionQueueReadException;
import java.util.Map;

/**
 * {@link AionMultiQueueReader.Mode#LATEST_ANY} coordination: every tick blocks only until at least
 * one stream has a new entry, then drains every stream that has new items up to its most recent one
 * and returns a row of the freshest value of every stream. Streams without a new entry keep their
 * prior value (so a row entry may be stale, or {@code null} if that stream has never produced). The
 * resolved tick timestamp is the newest timestamp across the whole row, which is monotonic
 * non-decreasing. End-of-stream is raised only once every subscribed stream is exhausted.
 *
 * <p>Unlike the other modes, no single sub-reader call may block — we must wake as soon as ANY
 * stream advances. Each subscribed writer signals a shared {@link AionWakeSignal} on publish/close;
 * this strategy registers that signal with every sub-reader at construction and unregisters it on
 * {@link #close()}, alternating between a non-blocking drain of every stream and an event-driven
 * park on the signal.
 *
 * @param <T> item type
 */
final class LatestAnyStrategy<T> implements TickStrategy<T> {

  private final Map<String, AionQueueReader<T>> readers;
  // Shared signal every subscribed writer notifies on publish/close; the reader parks on it.
  private final AionWakeSignal wake = new AionWakeSignal();

  LatestAnyStrategy(Map<String, AionQueueReader<T>> readers) {
    this.readers = readers;
    for (AionQueueReader<T> r : readers.values()) {
      r.registerWake(wake);
    }
  }

  @Override
  public long tick(Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    while (true) {
      long observed = wake.observe();
      boolean advanced = false;
      boolean allEndOfStream = true;
      for (AionQueueReader<T> r : readers.values()) {
        AionQueueReader.PollState s = r.drainToLatest();
        if (s == AionQueueReader.PollState.ADVANCED) {
          advanced = true;
          allEndOfStream = false;
        } else if (s == AionQueueReader.PollState.EMPTY) {
          allEndOfStream = false;
        }
      }
      if (advanced) {
        long rowT = Long.MIN_VALUE;
        for (AionQueueReader<T> r : readers.values()) {
          Long ct = r.currentT();
          if (ct != null && ct > rowT) rowT = ct;
        }
        return rowT;
      }
      if (allEndOfStream) {
        throw new AionEndOfStreamException("End of stream: all subscribed streams exhausted");
      }
      wake.await(observed, deadlineNanos);
    }
  }

  @Override
  public void close() {
    for (AionQueueReader<T> r : readers.values()) {
      r.unregisterWake(wake);
    }
  }
}
