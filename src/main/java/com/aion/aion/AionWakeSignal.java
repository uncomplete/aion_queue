package com.aion.aion;

import com.aion.aion.error.AionQueueReadException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Shared wake signal that lets a {@link AionMultiQueueReader.Mode#LATEST_ANY} multi-reader park
 * until <em>any</em> of its subscribed streams advances, rather than blocking on a single stream's
 * condition. The reader registers one signal with every subscribed {@link AionQueue}; each stream's
 * writer notifies it on publish/close.
 *
 * <p>Each multi-reader owns a private signal with exactly one waiter — its single owner thread — so
 * the wakeup is delivered with a lock-free {@link LockSupport#unpark} rather than a monitor. This
 * matters at scale: a single write fans out to one {@link #signal()} per subscribed {@code
 * LATEST_ANY} reader, and many writers fan into each, so keeping the notify path free of any shared
 * lock avoids serializing otherwise-independent writers (which each hold only their own {@code
 * queue.lock}).
 *
 * <p>Correctness rests on two pieces. A monotonic {@link #seq} counter makes the park immune to
 * lost wakeups: the reader {@link #observe()}s it before its non-blocking drain, then {@link
 * #await} parks only if it is still unchanged, re-checking once more after publishing itself as the
 * waiter. And {@code unpark}'s permit semantics close the remaining race — an {@code unpark} that
 * lands just before {@code park} leaves a permit that makes {@code park} return at once. The {@code
 * seq}/{@code waiter} handshake is sequentially consistent ({@link AtomicLong} + {@code volatile}),
 * so on every interleaving either the reader observes the bump and skips parking, or the writer
 * observes the waiter and unparks it.
 *
 * <p>Owned jointly by one reader thread (which parks) and any number of writer threads (which
 * signal).
 */
final class AionWakeSignal {

  private final AtomicLong seq = new AtomicLong();
  // The single parked reader thread, or null when it is not parked. Only ever written by that one
  // reader thread; read by any number of writer threads in signal().
  private volatile Thread waiter;

  /** Current sequence; capture before a non-blocking drain, then pass to {@link #await}. */
  long observe() {
    return seq.get();
  }

  /** Bump the sequence and wake the parked reader, if any. Called by writers on publish/close. */
  void signal() {
    seq.incrementAndGet();
    Thread w = waiter;
    if (w != null) {
      LockSupport.unpark(w);
    }
  }

  /**
   * Park until the sequence moves past {@code observed}, or the deadline passes. {@code
   * deadlineNanos == null} parks indefinitely. May return spuriously (a stale permit or a platform
   * spurious wakeup), so the caller re-polls under its outer loop.
   *
   * @throws AionQueueReadException if {@code deadlineNanos} has elapsed
   * @throws InterruptedException if the waiting thread is interrupted while parked
   */
  void await(long observed, Long deadlineNanos)
      throws InterruptedException, AionQueueReadException {
    if (seq.get() != observed) {
      return;
    }
    long remaining = 0L;
    if (deadlineNanos != null) {
      remaining = deadlineNanos - System.nanoTime();
      if (remaining <= 0) {
        throw new AionQueueReadException("Timeout waiting for any stream to update");
      }
    }
    waiter = Thread.currentThread();
    try {
      // Re-check after publishing the waiter: a signal() landing between the first check and the
      // publish could read waiter == null and skip the unpark, so we must catch its bump here.
      if (seq.get() != observed) {
        return;
      }
      if (deadlineNanos == null) {
        LockSupport.park(this);
      } else {
        LockSupport.parkNanos(this, remaining);
      }
    } finally {
      waiter = null;
    }
    // park()/parkNanos() return without throwing on interrupt; surface it as the contract requires
    // and clear the flag (Thread.interrupted()) so the outer loop cannot spin on a set flag.
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }
}
