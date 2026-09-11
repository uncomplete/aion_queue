package com.aion.aion;

import com.aion.aion.error.AionEndOfStreamException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A collection of named {@link AionQueue} streams sharing a common item type and timestamp
 * extractor. Each stream is an independent unbounded SPMC queue with its own writer; readers may
 * subscribe to every stream registered at attach time via {@link #createReader(int)}.
 *
 * @param <T> item type
 */
public final class AionMultiQueue<T> {

  private final Function<T, Long> tsExtractor;
  private final int defaultCapacity;
  private final ConcurrentHashMap<String, AionQueue<T>> queues = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AionQueueWriter<T>> writers = new ConcurrentHashMap<>();

  /** Multi-queue whose streams are all unbounded by default. */
  public AionMultiQueue(Function<T, Long> tsExtractor) {
    this(tsExtractor, Integer.MAX_VALUE);
  }

  /**
   * Multi-queue whose streams default to the given capacity. Individual streams may override via
   * {@link #createStream(String, int)}.
   */
  public AionMultiQueue(Function<T, Long> tsExtractor, int defaultCapacity) {
    if (tsExtractor == null) {
      throw new IllegalArgumentException("tsExtractor must not be null");
    }
    if (defaultCapacity < 2) {
      throw new IllegalArgumentException("defaultCapacity must be >= 2");
    }
    this.tsExtractor = tsExtractor;
    this.defaultCapacity = defaultCapacity;
  }

  /**
   * Register a new stream with the default capacity and return its writer. Throws if a stream with
   * this id already exists.
   */
  public AionQueueWriter<T> createStream(String streamId) {
    return createStream(streamId, defaultCapacity);
  }

  /** Register a new stream with the given capacity and return its writer. */
  public AionQueueWriter<T> createStream(String streamId, int capacity) {
    AionQueue<T> q = new AionQueue<>(tsExtractor, capacity);
    AionQueue<T> prev = queues.putIfAbsent(streamId, q);
    if (prev != null) {
      throw new IllegalStateException("Stream already exists: " + streamId);
    }
    AionQueueWriter<T> w = q.openWriter();
    writers.put(streamId, w);
    return w;
  }

  /**
   * Idempotent variant of {@link #createStream(String)}: returns the existing writer if the stream
   * is already registered, otherwise registers a new stream at the default capacity and returns its
   * writer. Intended for late-binding callers (e.g. consumers that discover topics dynamically)
   * that may ask for the same stream more than once.
   */
  public AionQueueWriter<T> openOrCreateStream(String streamId) {
    AionQueueWriter<T> existing = writers.get(streamId);
    if (existing != null) return existing;
    return createStream(streamId, defaultCapacity);
  }

  /**
   * Open a reader subscribed to every stream currently registered on this multi-queue, using the
   * default {@link AionMultiQueueReader.Mode#STRICT_ALIGNED} coordination.
   */
  public AionMultiQueueReader<T> createReader(int lookbackCapacity) {
    return createReader(lookbackCapacity, AionMultiQueueReader.Mode.STRICT_ALIGNED);
  }

  /**
   * Open a reader subscribed to every stream currently registered on this multi-queue, with the
   * given coordination {@link AionMultiQueueReader.Mode}. The set of subscribed streams is
   * snapshotted at call time — streams created later via {@link #createStream(String)} are NOT
   * visible to this reader; open a fresh reader to see them.
   *
   * <p>Each subscribed stream gets its own {@link AionQueueReader} with the supplied lookback
   * capacity.
   */
  public AionMultiQueueReader<T> createReader(
      int lookbackCapacity, AionMultiQueueReader.Mode mode) {
    Map<String, AionQueueReader<T>> readers = new LinkedHashMap<>();
    for (Map.Entry<String, AionQueue<T>> e : queues.entrySet()) {
      readers.put(e.getKey(), e.getValue().openReader(lookbackCapacity));
    }
    return new AionMultiQueueReader<>(readers, mode, lookbackCapacity);
  }

  /**
   * Open a reader subscribed to an explicit subset of streams. Every {@code streamId} must
   * correspond to a stream previously registered via {@link #createStream(String)}.
   */
  public AionMultiQueueReader<T> createReader(Collection<String> streamIds, int lookbackCapacity) {
    return createReader(streamIds, lookbackCapacity, AionMultiQueueReader.Mode.STRICT_ALIGNED);
  }

  /** Selective {@link #createReader(Collection, int)} with explicit mode. */
  public AionMultiQueueReader<T> createReader(
      Collection<String> streamIds, int lookbackCapacity, AionMultiQueueReader.Mode mode) {
    Map<String, AionQueueReader<T>> readers = new LinkedHashMap<>();
    for (String id : streamIds) {
      AionQueue<T> q = queues.get(id);
      if (q == null) {
        throw new IllegalArgumentException("Unknown stream: " + id);
      }
      readers.put(id, q.openReader(lookbackCapacity));
    }
    return new AionMultiQueueReader<>(readers, mode, lookbackCapacity);
  }

  /** Snapshot of registered stream ids. */
  public Set<String> streamIds() {
    return Set.copyOf(queues.keySet());
  }

  /**
   * Close every registered stream's writer. Active readers will drain any remaining buffered items
   * then receive {@link AionEndOfStreamException} on the next blocking call.
   * Idempotent.
   */
  public void closeAllWriters() {
    for (AionQueueWriter<T> w : writers.values()) {
      w.close();
    }
  }
}
