package com.aion.aion;

import com.aion.error.queue.AionEndOfStreamException;
import com.aion.error.queue.AionQueueReadException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Reader handle to an {@link AionMultiQueue}, holding one {@link AionQueueReader} per subscribed
 * stream. Cross-stream coordination is delegated to a {@link TickStrategy} selected by the {@link
 * Mode} chosen at {@code AionMultiQueue.createReader}:
 *
 * <ul>
 *   <li>{@link Mode#STRICT_ALIGNED}: every {@link #tick()} returns the next timestamp at which
 *       every subscribed stream has an item; sub-readers that lack that exact t advance to the
 *       smallest item at or above it (skipping intermediate items they uniquely had).
 *   <li>{@link Mode#TIME_MERGED}: every {@link #tick()} delivers one event in global time order.
 *       Blocks until every stream has a buffered next item, then advances only the stream(s) whose
 *       next-t equals the minimum across streams.
 *   <li>{@link Mode#LATEST_ANY}: every {@link #tick()} blocks only until at least one stream has a
 *       new entry, then drains every stream that has new items up to its most recent one and
 *       returns a row of the freshest value of every stream. Streams without a new entry keep their
 *       prior value (so a row entry may be stale, or {@code null} if that stream has never
 *       produced). The resolved tick timestamp is the newest timestamp across the whole row, which
 *       is monotonic non-decreasing. End-of-stream is raised only once every subscribed stream is
 *       exhausted.
 * </ul>
 *
 * <p>This class owns only coordination-agnostic plumbing — the sub-reader map, the resolved-tick
 * history, and row/lookback access. Each {@link Mode}'s behavior and any mode-specific state live in
 * its {@link TickStrategy}.
 *
 * @param <T> item type
 */
public final class AionMultiQueueReader<T> implements AutoCloseable {

  /**
   * Coordination strategy selector for {@link AionMultiQueueReader#tick()}. Each constant is a
   * factory for the {@link TickStrategy} that implements it; adding a mode is a new constant plus a
   * new strategy class, with no change to the reader.
   */
  public enum Mode {
    /** Every tick returns the smallest t at which all streams advance together. */
    STRICT_ALIGNED {
      @Override
      <T> TickStrategy<T> newStrategy(Map<String, AionQueueReader<T>> readers) {
        return new StrictAlignedStrategy<>(readers);
      }
    },
    /** Every tick delivers one event in global time order; only matching streams advance. */
    TIME_MERGED {
      @Override
      <T> TickStrategy<T> newStrategy(Map<String, AionQueueReader<T>> readers) {
        return new TimeMergedStrategy<>(readers);
      }
    },
    /** Every tick wakes as soon as any stream advances and snapshots the freshest value of all. */
    LATEST_ANY {
      @Override
      <T> TickStrategy<T> newStrategy(Map<String, AionQueueReader<T>> readers) {
        return new LatestAnyStrategy<>(readers);
      }
    };

    /** Create the coordination strategy for this mode, bound to {@code readers}. */
    abstract <T> TickStrategy<T> newStrategy(Map<String, AionQueueReader<T>> readers);
  }

  private final Map<String, AionQueueReader<T>> readers;
  private final Mode mode;
  private final TickStrategy<T> strategy;
  private final int lookbackCapacity;
  // History of resolved-tick timestamps (oldest first). Sized to lookbackCapacity + 1 so it tracks
  // sub-readers' lookback windows. An ArrayDeque so per-tick trimming of the oldest entry is O(1)
  // rather than the O(n) shift an ArrayList.remove(0) would cost. Confined to the single owner
  // thread (mutated by tick, read by stepsBack/currentT — caller-thread-confined contract).
  private final ArrayDeque<Long> tickHistory = new ArrayDeque<>();

  AionMultiQueueReader(Map<String, AionQueueReader<T>> readers, Mode mode, int lookbackCapacity) {
    this.readers = Collections.unmodifiableMap(readers);
    this.mode = mode;
    this.lookbackCapacity = lookbackCapacity;
    this.strategy = mode.newStrategy(this.readers);
  }

  /** The coordination mode this reader was created with. */
  public Mode mode() {
    return mode;
  }

  /**
   * Advance per the configured {@link Mode}. Returns the timestamp this tick resolved to.
   *
   * @throws InterruptedException if interrupted while waiting on any sub-reader
   * @throws AionEndOfStreamException if any subscribed stream cannot satisfy the tick
   */
  public long tick() throws InterruptedException, AionEndOfStreamException {
    try {
      return tickWithDeadline(null);
    } catch (AionQueueReadException impossible) {
      throw new AssertionError("untimed tick threw timeout", impossible);
    }
  }

  /**
   * Timed variant of {@link #tick()}. The timeout applies to the entire multi-tick operation: the
   * shared deadline is computed once and propagated to every sub-reader call.
   *
   * @throws AionQueueReadException if the timeout elapses before the tick converges
   */
  public long tick(Duration timeout)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    return tickWithDeadline(deadlineFrom(timeout));
  }

  private long tickWithDeadline(Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    long t = strategy.tick(deadlineNanos);
    recordTick(t);
    return t;
  }

  /** Append a resolved tick and trim the history to {@code lookbackCapacity + 1} entries. */
  private void recordTick(long t) {
    tickHistory.addLast(t);
    int maxSize = lookbackCapacity + 1;
    while (tickHistory.size() > maxSize) {
      tickHistory.removeFirst();
    }
  }

  private static Long deadlineFrom(Duration timeout) {
    if (timeout == null) {
      throw new IllegalArgumentException("timeout must not be null");
    }
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be non-negative");
    }
    long nanos = TimeUnit.NANOSECONDS.convert(timeout);
    return System.nanoTime() + nanos;
  }

  /**
   * Snapshot of every sub-reader's {@link AionQueueReader#current()} value, returned as a {@link
   * LinkedHashMap} in stream-registration order. Under {@link Mode#TIME_MERGED} an entry may be
   * {@code null} if that stream has not yet ticked.
   */
  public Map<String, T> currentRow() {
    return getRow(0);
  }

  /**
   * Snapshot of every sub-reader's value at {@code stepsBack} ticks before the current step.
   * Returned as a {@link LinkedHashMap} in stream-registration order. Entries may be {@code null}
   * for streams that lack a value at that position.
   */
  public Map<String, T> getRow(int stepsBack) {
    if (stepsBack < 0) {
      throw new IllegalArgumentException("stepsBack must be >= 0");
    }
    Map<String, T> row = new LinkedHashMap<>(readers.size());
    for (Map.Entry<String, AionQueueReader<T>> e : readers.entrySet()) {
      row.put(e.getKey(), e.getValue().get(stepsBack));
    }
    return row;
  }

  /** Timestamp of the most recently resolved tick, or {@code null} before the first tick. */
  public Long currentT() {
    return tickHistory.isEmpty() ? null : tickHistory.getLast();
  }

  /**
   * Returns the number of resolved-tick steps back from {@link #currentT()} at which {@code
   * targetT} appears in the tick history. {@code targetT == currentT()} returns 0.
   *
   * @throws IllegalArgumentException if {@code targetT} is not in this multi-reader's tick history
   */
  public long stepsBack(long targetT) {
    long back = 0;
    for (Iterator<Long> it = tickHistory.descendingIterator(); it.hasNext(); back++) {
      long t = it.next();
      if (t == targetT) return back;
      if (t < targetT) break;
    }
    throw new IllegalArgumentException("targetT " + targetT + " not in multi-reader tick history");
  }

  /** Returns the underlying {@link AionQueueReader} for the given stream. */
  public AionQueueReader<T> reader(String streamId) {
    AionQueueReader<T> r = readers.get(streamId);
    if (r == null) {
      throw new IllegalArgumentException("Not subscribed to stream: " + streamId);
    }
    return r;
  }

  /** {@link AionQueueReader#current()} on the named stream. */
  public T current(String streamId) {
    return reader(streamId).current();
  }

  /** {@link AionQueueReader#get(int)} on the named stream. */
  public T get(String streamId, int stepsBack) {
    return reader(streamId).get(stepsBack);
  }

  /** The set of streams this reader is subscribed to. */
  public Set<String> streamIds() {
    return readers.keySet();
  }

  /** Closes the coordination strategy (releasing any wake signal), then every sub-reader. */
  @Override
  public void close() {
    strategy.close();
    for (AionQueueReader<T> r : readers.values()) {
      r.close();
    }
  }
}
