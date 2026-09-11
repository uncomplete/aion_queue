package com.aion.aion;

import com.aion.aion.error.AionEndOfStreamException;
import com.aion.aion.error.AionQueueReadException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * {@link AionMultiQueueReader.Mode#TIME_MERGED} coordination: every tick delivers one event in
 * global time order. Blocks until every stream has a buffered next item, then advances only the
 * stream(s) whose next-t equals the minimum across streams.
 *
 * <p>Implemented as a k-way merge over a persistent min-heap of stream heads, keyed by next-t. Each
 * tick pops every entry equal to the global minimum and advances those stream(s); the rest stay
 * cached in the heap, so per tick we re-peek only the stream(s) just consumed instead of all k. Cost
 * drops from O(k) (linear scan + a fresh map allocation every tick) to O(m log k), where m is the
 * number of streams sharing the minimum (typically 1).
 *
 * <p>EOS/timeout timing is preserved by <em>deferred</em> refresh: a consumed stream is queued in
 * {@code mergePending} and re-peeked at the START of the NEXT tick. That is exactly when the linear
 * version would have blocked on (or raised EOS from) that stream's now-empty head, so a stream's
 * final item is still returned on the tick before EOS surfaces. On a peek that blocks/throws
 * (timeout or EOS), the un-peeked id stays in {@code mergePending}, so a retried tick resumes
 * cleanly with no duplicates.
 *
 * <p>A stream is always in exactly one of {@code mergeHeap}, {@code mergePending}. State is confined
 * to the reader's single owner thread.
 *
 * @param <T> item type
 */
final class TimeMergedStrategy<T> implements TickStrategy<T> {

  /** One cached stream head for the min-heap. */
  private record MergeEntry(long t, String id) {}

  private final Map<String, AionQueueReader<T>> readers;
  private final PriorityQueue<MergeEntry> mergeHeap =
      new PriorityQueue<>(Comparator.comparingLong(MergeEntry::t));
  private final ArrayDeque<String> mergePending = new ArrayDeque<>();
  private boolean mergeStarted = false;

  TimeMergedStrategy(Map<String, AionQueueReader<T>> readers) {
    this.readers = readers;
  }

  @Override
  public long tick(Long deadlineNanos)
      throws InterruptedException, AionEndOfStreamException, AionQueueReadException {
    if (!mergeStarted) {
      mergePending.addAll(readers.keySet()); // refresh every stream on the first tick
      mergeStarted = true;
    }
    // Refresh streams consumed last tick (all of them on the first tick). peekNextTInternal
    // blocks/raises here, matching the linear version's timing; peek before removal so a throw
    // leaves the id queued for retry.
    while (!mergePending.isEmpty()) {
      String id = mergePending.peekLast();
      long head = readers.get(id).peekNextTInternal(deadlineNanos);
      mergePending.removeLast();
      mergeHeap.add(new MergeEntry(head, id));
    }
    if (mergeHeap.isEmpty()) return Long.MAX_VALUE; // no subscribed streams
    long minT = mergeHeap.peek().t();
    while (!mergeHeap.isEmpty() && mergeHeap.peek().t() == minT) {
      MergeEntry e = mergeHeap.poll();
      readers.get(e.id()).tickToInternal(Long.MIN_VALUE, deadlineNanos);
      mergePending.addLast(e.id());
    }
    return minT;
  }
}
