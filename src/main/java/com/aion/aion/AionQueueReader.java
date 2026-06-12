package com.aion.aion;

import com.aion.error.queue.AionEndOfStreamException;
import com.aion.error.queue.AionQueueReadException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Reader handle to an {@link AionQueue}. Items written after this reader's attach time are consumed
 * in monotonic timestamp order; {@link #tick()} returns the timestamp of the next item, and {@link
 * #tickTo(long)} skips ahead to the smallest item with {@code t >= target}.
 *
 * <p>Every blocking call has a {@link Duration} overload that throws {@link AionQueueReadException}
 * once the supplied timeout elapses.
 *
 * <p>Multiple readers may coexist on the same queue, each with an independent cursor and its own
 * lookback. Recently consumed items are retained in a sliding window for access via {@link
 * #get(int)} / {@link #current()}.
 *
 * <p>Owned by a single caller thread. Constructed via {@link AionQueue#openReader(int)}.
 *
 * @param <T> item type
 */
public final class AionQueueReader<T> implements AutoCloseable {

  private final AionQueue<T> queue;

  // Sliding window of the most recently consumed items (oldest at logical index 0). Fixed capacity
  // lookbackCapacity + 1; a ring buffer so per-tick append + oldest-eviction is O(1) rather than the
  // O(n) shift an ArrayList.remove(0) would cost on every tick.
  private final Lookback<T> lookback;
  private Long lastConsumedT;
  private volatile boolean closed = false;

  AionQueueReader(AionQueue<T> queue, int lookbackCapacity, Long startCursor) {
    if (lookbackCapacity < 0) {
      throw new IllegalArgumentException("lookbackCapacity must be >= 0");
    }
    this.queue = queue;
    this.lookback = new Lookback<>(lookbackCapacity + 1);
    this.lastConsumedT = startCursor;
  }

  // -------- tick (untimed) --------

  /**
   * Advance to the next item (smallest {@code t > lastConsumedT}). Blocks indefinitely until such
   * an item is available; throws if the writer has closed and no further item satisfies.
   */
  public long tick() throws InterruptedException, AionEndOfStreamException {
    try {
      return tickToInternal(Long.MIN_VALUE, null);
    } catch (AionQueueReadException impossible) {
      throw new AssertionError("untimed tick threw timeout", impossible);
    }
  }

  /**
   * Timed variant of {@link #tick()}. Throws {@link AionQueueReadException} once {@code timeout}
   * elapses with no satisfying item available.
   */
  public long tick(Duration timeout)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    return tickToInternal(Long.MIN_VALUE, deadlineFrom(timeout));
  }

  // -------- tickTo (untimed) --------

  /**
   * Advance to the smallest item with {@code t >= max(target, lastConsumedT + 1)}. Blocks
   * indefinitely until such an item is available; throws if the writer has closed and no further
   * satisfying item exists. A {@code target} at or below the current cursor is equivalent to {@link
   * #tick()}.
   */
  public long tickTo(long target) throws InterruptedException, AionEndOfStreamException {
    try {
      return tickToInternal(target, null);
    } catch (AionQueueReadException impossible) {
      throw new AssertionError("untimed tickTo threw timeout", impossible);
    }
  }

  /**
   * Timed variant of {@link #tickTo(long)}. Throws {@link AionQueueReadException} once {@code
   * timeout} elapses with no satisfying item available.
   */
  public long tickTo(long target, Duration timeout)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    return tickToInternal(target, deadlineFrom(timeout));
  }

  // -------- peekNextT (untimed + timed) --------

  /**
   * Blocking lookahead: returns the timestamp of the next item this reader would consume, without
   * advancing the cursor or lookback. Blocks indefinitely until such an item is available.
   */
  public long peekNextT() throws InterruptedException, AionEndOfStreamException {
    try {
      return peekNextTInternal(null);
    } catch (AionQueueReadException impossible) {
      throw new AssertionError("untimed peekNextT threw timeout", impossible);
    }
  }

  /**
   * Timed variant of {@link #peekNextT()}. Throws {@link AionQueueReadException} once {@code
   * timeout} elapses with no satisfying item available.
   */
  public long peekNextT(Duration timeout)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    return peekNextTInternal(deadlineFrom(timeout));
  }

  // -------- core loops --------

  /**
   * Core advance loop. {@code deadlineNanos == null} means block indefinitely; otherwise wait until
   * that absolute {@link System#nanoTime()} before throwing.
   */
  long tickToInternal(long target, Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    queue.lock.lock();
    try {
      long effective = lastConsumedT == null ? target : Math.max(target, lastConsumedT + 1);
      while (true) {
        if (closed) throw new AionEndOfStreamException("Reader is closed");
        Map.Entry<Long, T> entry = queue.buffer.ceilingEntry(effective);
        if (entry != null) {
          long t = entry.getKey();
          T value = entry.getValue();
          lastConsumedT = t;
          lookback.add(value);
          queue.evict();
          return t;
        }
        if (queue.writerClosed
            && (queue.latestWrittenT == null || queue.latestWrittenT < effective)) {
          throw new AionEndOfStreamException(
              "End of stream: target=" + target + ", latestWrittenT=" + queue.latestWrittenT);
        }
        if (deadlineNanos == null) {
          queue.notEmpty.await();
        } else {
          long remaining = deadlineNanos - System.nanoTime();
          if (remaining <= 0) {
            throw new AionQueueReadException("Timeout waiting for item: target=" + target);
          }
          queue.notEmpty.awaitNanos(remaining);
        }
      }
    } finally {
      queue.lock.unlock();
    }
  }

  /** Outcome of a {@link #drainToLatest()} non-blocking advance attempt. */
  enum PollState {
    /** The cursor advanced by one item. */
    ADVANCED,
    /** No item available yet, but the stream is still open. */
    EMPTY,
    /** The writer has closed and no further item satisfies this reader. */
    END_OF_STREAM
  }

  /**
   * Non-blocking drain to the newest buffered item, for {@link
   * AionMultiQueueReader.Mode#LATEST_ANY}. Consumes every item currently buffered for this reader
   * (from the smallest {@code t > lastConsumedT} onward) under a single lock acquisition, evicting
   * once at the end rather than re-locking and re-evicting per item. Reports {@link
   * PollState#ADVANCED} if at least one item was consumed; otherwise {@link PollState#EMPTY} (stream
   * open, nothing ready) or {@link PollState#END_OF_STREAM} (writer closed and fully drained). Never
   * blocks.
   *
   * <p>As with the per-item version, end-of-stream is deferred: a drain that consumes a stream's
   * final item returns {@link PollState#ADVANCED}, and {@link PollState#END_OF_STREAM} surfaces only
   * on a subsequent call that consumes nothing.
   */
  PollState drainToLatest() {
    queue.lock.lock();
    try {
      if (closed) return PollState.END_OF_STREAM;
      boolean advanced = false;
      while (true) {
        long effective = lastConsumedT == null ? Long.MIN_VALUE : lastConsumedT + 1;
        Map.Entry<Long, T> entry = queue.buffer.ceilingEntry(effective);
        if (entry != null) {
          lastConsumedT = entry.getKey();
          lookback.add(entry.getValue());
          advanced = true;
          continue; // keep draining to the most recent item
        }
        if (advanced) {
          queue.evict(); // evict once for the whole drain, not per item
          return PollState.ADVANCED;
        }
        if (queue.writerClosed
            && (queue.latestWrittenT == null || queue.latestWrittenT < effective)) {
          return PollState.END_OF_STREAM;
        }
        return PollState.EMPTY;
      }
    } finally {
      queue.lock.unlock();
    }
  }

  /** Registers {@code wake} so this reader's writer signals it on publish/close. */
  void registerWake(AionWakeSignal wake) {
    queue.registerWake(wake);
  }

  /** Stops {@code wake} from being signalled by this reader's writer. Idempotent. */
  void unregisterWake(AionWakeSignal wake) {
    queue.unregisterWake(wake);
  }

  /** Core lookahead loop. {@code deadlineNanos == null} means block indefinitely. */
  long peekNextTInternal(Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    queue.lock.lock();
    try {
      long effective = lastConsumedT == null ? Long.MIN_VALUE : lastConsumedT + 1;
      while (true) {
        if (closed) throw new AionEndOfStreamException("Reader is closed");
        Map.Entry<Long, T> entry = queue.buffer.ceilingEntry(effective);
        if (entry != null) {
          return entry.getKey();
        }
        if (queue.writerClosed
            && (queue.latestWrittenT == null || queue.latestWrittenT < effective)) {
          throw new AionEndOfStreamException(
              "End of stream: latestWrittenT=" + queue.latestWrittenT);
        }
        if (deadlineNanos == null) {
          queue.notEmpty.await();
        } else {
          long remaining = deadlineNanos - System.nanoTime();
          if (remaining <= 0) {
            throw new AionQueueReadException("Timeout waiting for next item to peek");
          }
          queue.notEmpty.awaitNanos(remaining);
        }
      }
    } finally {
      queue.lock.unlock();
    }
  }

  // -------- accessors --------

  /** The most recently ticked item, or {@code null} before the first successful tick. */
  public T current() {
    return get(0);
  }

  /**
   * Returns the item at {@code stepsBack} ticks before {@link #current()}. {@code stepsBack == 0}
   * returns {@link #current()}. Returns {@code null} if {@code stepsBack} exceeds the items
   * currently retained in the lookback window.
   */
  public T get(int stepsBack) {
    if (stepsBack < 0) {
      throw new IllegalArgumentException("stepsBack must be >= 0");
    }
    queue.lock.lock();
    try {
      int idx = lookback.size() - 1 - stepsBack;
      if (idx < 0) return null;
      return lookback.get(idx);
    } finally {
      queue.lock.unlock();
    }
  }

  /** The timestamp of the most recently ticked item, or {@code null} before the first tick. */
  public Long currentT() {
    queue.lock.lock();
    try {
      return lastConsumedT == null || lookback.isEmpty() ? null : lastConsumedT;
    } finally {
      queue.lock.unlock();
    }
  }

  /**
   * Returns the number of ticks back at which the item with timestamp {@code targetT} sits in this
   * reader's lookback window. {@code targetT == currentT()} returns 0; the immediately previous
   * lookback entry returns 1; etc.
   *
   * @throws IllegalArgumentException if {@code targetT} is not present in the lookback window
   */
  public long stepsBack(long targetT) {
    queue.lock.lock();
    try {
      for (int i = lookback.size() - 1; i >= 0; i--) {
        long t = queue.tsExtractor.apply(lookback.get(i));
        if (t == targetT) return lookback.size() - 1 - i;
        if (t < targetT) break;
      }
      throw new IllegalArgumentException(
          "targetT " + targetT + " not in lookback window of reader");
    } finally {
      queue.lock.unlock();
    }
  }

  /**
   * Releases the reader. Any in-flight blocking call wakes and throws {@link
   * AionEndOfStreamException}.
   */
  @Override
  public void close() {
    if (closed) return;
    closed = true;
    queue.lock.lock();
    try {
      queue.notEmpty.signalAll();
    } finally {
      queue.lock.unlock();
    }
    queue.unregisterReader(this);
  }

  /**
   * Smallest timestamp this reader still needs in the buffer. Used by {@link AionQueue#evict()}.
   * Caller must hold {@link AionQueue#lock}.
   */
  long oldestNeededTLocked() {
    if (lookback.isEmpty()) {
      return lastConsumedT == null ? Long.MIN_VALUE : lastConsumedT + 1;
    }
    return queue.tsExtractor.apply(lookback.get(0));
  }

  // -------- helpers --------

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
   * Fixed-capacity ring buffer of the reader's most recently consumed items, oldest at logical
   * index 0 and newest at {@code size() - 1}. Sized once to {@code lookbackCapacity + 1}; appending
   * once full overwrites the oldest entry. Append, oldest-eviction, and indexed access are all O(1),
   * avoiding the O(n) element shift an {@code ArrayList.remove(0)} incurs on every tick. Guarded by
   * the enclosing reader's {@link AionQueue#lock}, like every other access to the lookback window.
   */
  private static final class Lookback<T> {

    private final Object[] items;
    private int head; // logical index 0 maps to items[head]
    private int size;

    Lookback(int capacity) {
      this.items = new Object[capacity];
    }

    void add(T value) {
      if (size < items.length) {
        items[(head + size) % items.length] = value;
        size++;
      } else {
        // Full: overwrite the oldest entry and advance the head past it.
        items[head] = value;
        head = (head + 1) % items.length;
      }
    }

    int size() {
      return size;
    }

    boolean isEmpty() {
      return size == 0;
    }

    /** Item at logical index {@code i} (0 = oldest retained, {@code size() - 1} = newest). */
    @SuppressWarnings("unchecked")
    T get(int i) {
      return (T) items[(head + i) % items.length];
    }
  }
}
