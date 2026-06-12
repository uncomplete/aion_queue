package com.aion.aion;

import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Single-producer / multi-consumer queue with broadcast (fan-out) semantics, keyed by a {@code
 * long} timestamp extracted from each item. Items are written in strictly increasing timestamp
 * order; every attached {@link AionQueueReader} sees every item written after its attach time and
 * can advance to a specific timestamp via {@link AionQueueReader#tickTo(long)}.
 *
 * <p>Construct via {@code new AionQueue<>(tsExtractor)} for an unbounded queue, or {@code new
 * AionQueue<>(tsExtractor, capacity)} for a bounded queue that backpressures the writer when the
 * buffer fills. Attach exactly one {@link AionQueueWriter} via {@link #openWriter()} and any number
 * of readers via {@link #openReader(int)}.
 *
 * <p><b>Capacity caveat:</b> the writer can only make progress past pinned items once at least one
 * reader's lookback window has slid forward. With a slow reader of {@code lookbackCapacity = L},
 * the queue must be sized with {@code capacity > L + 1}; otherwise the writer parks indefinitely
 * (or times out) because the reader can't advance without new items.
 *
 * @param <T> item type
 */
public final class AionQueue<T> {

  final ReentrantLock lock = new ReentrantLock();
  final Condition notEmpty = lock.newCondition();
  final Condition notFull = lock.newCondition();

  final Function<T, Long> tsExtractor;
  final int capacity;
  // Buffer keyed by the extracted timestamp. Guarded by lock.
  final TreeMap<Long, T> buffer = new TreeMap<>();
  Long latestWrittenT = null;

  volatile boolean writerClosed = false;

  final CopyOnWriteArrayList<AionQueueReader<T>> readers = new CopyOnWriteArrayList<>();
  // Wake signals of subscribed LATEST_ANY multi-readers, notified on every publish/close so they
  // can park until any stream advances. Empty (zero overhead) unless such a reader is attached.
  final CopyOnWriteArrayList<AionWakeSignal> wakeSignals = new CopyOnWriteArrayList<>();
  private final AtomicReference<AionQueueWriter<T>> writer = new AtomicReference<>();

  /** Unbounded queue (capacity = {@link Integer#MAX_VALUE}). */
  public AionQueue(Function<T, Long> tsExtractor) {
    this(tsExtractor, Integer.MAX_VALUE);
  }

  /**
   * Bounded queue: when {@code buffer.size() >= capacity} the writer parks until reader-driven
   * eviction frees space (or the writer's timeout elapses). Each reader's {@code lookbackCapacity}
   * must satisfy {@code lookbackCapacity + 2 <= capacity} or the reader will be rejected at {@link
   * #openReader(int)}.
   *
   * @param capacity maximum number of items the buffer may hold; must be at least 2
   */
  public AionQueue(Function<T, Long> tsExtractor, int capacity) {
    if (tsExtractor == null) {
      throw new IllegalArgumentException("tsExtractor must not be null");
    }
    if (capacity < 2) {
      throw new IllegalArgumentException(
          "capacity must be >= 2 (needed to allow reader lookback to slide)");
    }
    this.tsExtractor = tsExtractor;
    this.capacity = capacity;
  }

  /** Open the queue's single writer. Throws if a writer is already attached. */
  public AionQueueWriter<T> openWriter() {
    AionQueueWriter<T> w = new AionQueueWriter<>(this);
    if (!writer.compareAndSet(null, w)) {
      throw new IllegalStateException("AionQueue already has a writer");
    }
    return w;
  }

  /**
   * Attach a new reader with a sliding lookback window of the given capacity. The reader sees items
   * written after its attach time (items already in the buffer are not visible to it). {@code
   * lookbackCapacity == 0} retains only the current item.
   *
   * <p>For bounded queues the reader's lookback must leave room for at least one new write — {@code
   * lookbackCapacity + 2 <= capacity}, or this method throws.
   */
  public AionQueueReader<T> openReader(int lookbackCapacity) {
    if (lookbackCapacity > capacity - 2) {
      throw new IllegalArgumentException(
          "lookbackCapacity ("
              + lookbackCapacity
              + ") + 2 must be <= queue capacity ("
              + capacity
              + ") so the lookback can slide forward");
    }
    lock.lock();
    try {
      AionQueueReader<T> r = new AionQueueReader<>(this, lookbackCapacity, latestWrittenT);
      readers.add(r);
      return r;
    } finally {
      lock.unlock();
    }
  }

  void unregisterWriter(AionQueueWriter<T> w) {
    writer.compareAndSet(w, null);
  }

  void registerWake(AionWakeSignal w) {
    wakeSignals.addIfAbsent(w);
  }

  void unregisterWake(AionWakeSignal w) {
    wakeSignals.remove(w);
  }

  /** Notify every registered wake signal that this stream's state changed (a write or close). */
  void signalWake() {
    for (AionWakeSignal w : wakeSignals) {
      w.signal();
    }
  }

  void unregisterReader(AionQueueReader<T> r) {
    readers.remove(r);
    lock.lock();
    try {
      evict();
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Caller must hold {@link #lock}. Drops buffer entries below the slowest reader's pin and signals
   * {@link #notFull} if any items were freed.
   */
  void evict() {
    int sizeBefore = buffer.size();
    long floor = Long.MAX_VALUE;
    boolean anyReader = false;
    for (AionQueueReader<T> r : readers) {
      anyReader = true;
      long p = r.oldestNeededTLocked();
      if (p < floor) floor = p;
    }
    if (anyReader) {
      buffer.headMap(floor).clear();
    } else {
      // No readers means no one can ever consume what's buffered; drop it all so the writer
      // can keep making progress (late readers attach at latestWrittenT and don't see prior items).
      buffer.clear();
    }
    if (buffer.size() < sizeBefore) {
      notFull.signalAll();
    }
  }
}
