package com.aion.aion;

import com.aion.error.queue.AionQueueMonotonicityException;
import com.aion.error.queue.AionQueueWriteException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Single-writer handle to an {@link AionQueue}. Writes are ordered by extracted timestamp; each
 * write must produce a timestamp strictly greater than the previous one. If the queue is bounded,
 * writes block (or time out) until the slowest reader's lookback slides forward to free space.
 *
 * @param <T> item type
 */
public final class AionQueueWriter<T> implements AutoCloseable {

  private final AionQueue<T> queue;
  private volatile boolean closed = false;

  AionQueueWriter(AionQueue<T> queue) {
    this.queue = queue;
  }

  /**
   * The timestamp of the most recently written item, or {@code null} if nothing has been written.
   */
  public Long lastWrittenT() {
    queue.lock.lock();
    try {
      return queue.latestWrittenT;
    } finally {
      queue.lock.unlock();
    }
  }

  /**
   * Append a value. Blocks indefinitely if the queue is at capacity, until reader-driven eviction
   * frees space.
   *
   * @throws AionQueueMonotonicityException if the extracted timestamp is not strictly greater than
   *     the previous write
   * @throws InterruptedException if interrupted while parked under backpressure
   */
  public void write(T value) throws AionQueueMonotonicityException, InterruptedException {
    try {
      writeInternal(value, null);
    } catch (AionQueueWriteException impossible) {
      throw new AssertionError("untimed write threw backpressure", impossible);
    }
  }

  /**
   * Timed variant: throws {@link AionQueueWriteException} if {@code timeout} elapses with the queue
   * still at capacity.
   */
  public void write(T value, Duration timeout)
      throws AionQueueMonotonicityException, InterruptedException, AionQueueWriteException {
    writeInternal(value, deadlineFrom(timeout));
  }

  private void writeInternal(T value, Long deadlineNanos)
      throws AionQueueMonotonicityException, InterruptedException, AionQueueWriteException {
    if (closed) {
      throw new IllegalStateException("AionQueueWriter is closed");
    }
    long t = queue.tsExtractor.apply(value);
    queue.lock.lock();
    try {
      if (queue.latestWrittenT != null && t <= queue.latestWrittenT) {
        throw new AionQueueMonotonicityException(queue.latestWrittenT, t);
      }
      while (queue.buffer.size() >= queue.capacity) {
        if (deadlineNanos == null) {
          queue.notFull.await();
        } else {
          long remaining = deadlineNanos - System.nanoTime();
          if (remaining <= 0) {
            throw new AionQueueWriteException(queue.buffer.size(), queue.capacity);
          }
          queue.notFull.awaitNanos(remaining);
        }
      }
      queue.buffer.put(t, value);
      queue.latestWrittenT = t;
      queue.notEmpty.signalAll();
      queue.signalWake();
    } finally {
      queue.lock.unlock();
    }
  }

  /**
   * Marks the writer closed. Readers blocked in {@code tick()} / {@code tickTo()} will receive
   * {@link com.aion.error.queue.AionEndOfStreamException} once they have drained past their
   * requested target.
   */
  @Override
  public void close() {
    if (closed) return;
    closed = true;
    queue.lock.lock();
    try {
      queue.writerClosed = true;
      queue.notEmpty.signalAll();
      queue.signalWake();
    } finally {
      queue.lock.unlock();
    }
    queue.unregisterWriter(this);
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
}
