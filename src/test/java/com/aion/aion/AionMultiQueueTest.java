package com.aion.aion;

import static org.junit.jupiter.api.Assertions.*;

import com.aion.aion.error.AionEndOfStreamException;
import com.aion.aion.error.AionQueueReadException;
import com.aion.aion.error.AionQueueWriteException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AionMultiQueueTest {

  private static final Function<Integer, Long> INT_TS = i -> (long) i;

  private static AionMultiQueue<Integer> newMQ() {
    return new AionMultiQueue<>(INT_TS);
  }

  // -------- createStream --------

  @Test
  void createStreamRegistersAndReturnsWriter() {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> w = mq.createStream("a");
    assertNotNull(w);
    assertEquals(java.util.Set.of("a"), mq.streamIds());
  }

  @Test
  void createStreamRejectsDuplicateId() {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    assertThrows(IllegalStateException.class, () -> mq.createStream("a"));
  }

  @Test
  void nullExtractorRejected() {
    assertThrows(IllegalArgumentException.class, () -> new AionMultiQueue<Integer>(null));
  }

  // -------- createReader: snapshot semantics --------

  @Test
  void createReaderSubscribesToAllExistingStreams() {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    mq.createStream("b");
    mq.createStream("c");
    AionMultiQueueReader<Integer> r = mq.createReader(0);
    assertEquals(java.util.Set.of("a", "b", "c"), r.streamIds());
  }

  @Test
  void streamsAddedAfterCreateReaderAreInvisibleToThatReader() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");

    AionMultiQueueReader<Integer> early = mq.createReader(0);
    assertEquals(java.util.Set.of("a"), early.streamIds());

    AionQueueWriter<Integer> wb = mq.createStream("b");
    assertEquals(java.util.Set.of("a"), early.streamIds());
    assertThrows(IllegalArgumentException.class, () -> early.current("b"));

    AionMultiQueueReader<Integer> late = mq.createReader(0);
    assertEquals(java.util.Set.of("a", "b"), late.streamIds());

    // early can still tick its one stream.
    wa.write(1);
    wb.write(99);
    assertEquals(1L, early.tick());
    assertEquals(1, early.current("a"));
  }

  // -------- Lockstep tick by timestamp --------

  @Test
  void tickAdvancesAllSubReadersToSameTimestampWhenAligned() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    // Aligned timestamps across both streams.
    wa.write(5);
    wb.write(5);
    wa.write(7);
    wb.write(7);

    assertEquals(5L, r.tick());
    assertEquals(5, r.current("a"));
    assertEquals(5, r.current("b"));

    assertEquals(7L, r.tick());
    assertEquals(7, r.current("a"));
    assertEquals(7, r.current("b"));
  }

  @Test
  void tickConvergesWhenStreamsHaveDifferentTimestamps() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    // A: {10, 30}; B: {15, 25, 30}. First multi-tick should converge to 30:
    // - tick a → 10; tick b → 15; target = 15
    // - a.lastConsumedT(10) < 15: a.tickTo(15) → 30; target = 30 → restart
    // - b.lastConsumedT(15) < 30: b.tickTo(30) → 30 (need that 30 to exist)
    wa.write(10);
    wa.write(30);
    wb.write(15);
    wb.write(25);
    wb.write(30);

    assertEquals(30L, r.tick());
    assertEquals(30, r.current("a"));
    assertEquals(30, r.current("b"));
  }

  @Test
  void tickBlocksUntilSlowestStreamCanReachTarget() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    // A leaps to 30; B only has 15 — B must wait for an item >= 30.
    wa.write(10);
    wa.write(30);
    wb.write(15);

    AtomicBoolean done = new AtomicBoolean(false);
    AtomicLong result = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    result.set(r.tick());
                    done.set(true);
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(100);
    assertFalse(done.get(), "tick should be parked waiting on B's catch-up");

    wb.write(35); // satisfies tickTo(30) → returns 35; A then catches up via tickTo(35)
    wa.write(35);
    t.join(2000);
    assertEquals(35L, result.get());
    assertEquals(35, r.current("a"));
    assertEquals(35, r.current("b"));
  }

  @Test
  void tickThrowsEosWhenAnyStreamCannotReachTarget() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    wa.write(10);
    wa.write(30);
    wb.write(15);
    wb.close(); // B has 15 only — drained.

    // First multi-tick: a→10, b→15, target=15. a.tickTo(15)→30, target=30.
    // b.tickTo(30) finds nothing and throws AEOS.
    assertThrows(AionEndOfStreamException.class, r::tick);
  }

  // -------- currentRow --------

  @Test
  void currentRowReturnsSnapshotInRegistrationOrder() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionQueueWriter<Integer> wc = mq.createStream("c");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    wa.write(5);
    wb.write(5);
    wc.write(5);
    assertEquals(5L, r.tick());

    Map<String, Integer> row = r.currentRow();
    assertEquals(List.of("a", "b", "c"), List.copyOf(row.keySet()));
    assertEquals(5, row.get("a"));
    assertEquals(5, row.get("b"));
    assertEquals(5, row.get("c"));
  }

  @Test
  void currentRowBeforeFirstTickHoldsNulls() {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    Map<String, Integer> row = r.currentRow();
    assertNull(row.get("a"));
    assertNull(row.get("b"));
    assertEquals(2, row.size());
  }

  // -------- Per-stream lookback access --------

  @Test
  void perStreamLookbackReflectsCoordinatedSteps() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(2); // current + 2 back

    for (int i = 1; i <= 4; i++) {
      wa.write(i);
      wb.write(i);
    }
    for (int i = 0; i < 4; i++) assertEquals((long) (i + 1), r.tick());

    assertEquals(4, r.current("a"));
    assertEquals(3, r.get("a", 1));
    assertEquals(2, r.get("a", 2));
    assertNull(r.get("a", 3));

    assertEquals(4, r.current("b"));
    assertEquals(3, r.get("b", 1));
  }

  @Test
  void readerAndCurrentRejectUnknownStream() {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    AionMultiQueueReader<Integer> r = mq.createReader(0);
    assertThrows(IllegalArgumentException.class, () -> r.reader("b"));
    assertThrows(IllegalArgumentException.class, () -> r.current("b"));
    assertThrows(IllegalArgumentException.class, () -> r.get("b", 0));
  }

  // -------- Merge mode --------

  @Test
  void mergeTickDeliversEventsInGlobalTimeOrder() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.TIME_MERGED);

    // A: {10, 30}; B: {15, 25, 30}.
    wa.write(10);
    wa.write(30);
    wb.write(15);
    wb.write(25);
    wb.write(30);

    // Tick 1: min(10, 15) = 10 → advance A → A=10, B=null
    assertEquals(10L, r.tick());
    assertEquals(10, r.current("a"));
    assertNull(r.current("b"));

    // Tick 2: A.next=30, B.next=15. Min=15 → advance B → A=10, B=15
    assertEquals(15L, r.tick());
    assertEquals(10, r.current("a"));
    assertEquals(15, r.current("b"));

    // Tick 3: A.next=30, B.next=25. Min=25 → advance B → A=10, B=25
    assertEquals(25L, r.tick());
    assertEquals(10, r.current("a"));
    assertEquals(25, r.current("b"));

    // Tick 4: A.next=30, B.next=30. Min=30 (tie) → advance both → A=30, B=30
    assertEquals(30L, r.tick());
    assertEquals(30, r.current("a"));
    assertEquals(30, r.current("b"));
  }

  @Test
  void mergeTickBlocksUntilEveryStreamHasData() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.TIME_MERGED);

    // Only A has data; merge tick must block until B writes something.
    wa.write(10);

    AtomicBoolean done = new AtomicBoolean(false);
    AtomicLong result = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    result.set(r.tick());
                    done.set(true);
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(100);
    assertFalse(done.get(), "merge tick should be parked waiting on B");

    wb.write(5); // smaller than A's 10
    t.join(2000);
    assertEquals(5L, result.get());
    assertNull(r.current("a"));
    assertEquals(5, r.current("b"));
  }

  @Test
  void mergeTickThrowsEosWhenAnyStreamIsDrained() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.TIME_MERGED);

    wa.write(10);
    wb.close(); // B never gets any data.

    assertThrows(AionEndOfStreamException.class, r::tick);
  }

  @Test
  void mergeTickReturnsSameTimestampAcrossSimultaneousEvents() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionQueueWriter<Integer> wc = mq.createStream("c");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.TIME_MERGED);

    // All three streams have an event at t=7 → single tick advances all three.
    wa.write(7);
    wb.write(7);
    wc.write(7);
    assertEquals(7L, r.tick());
    assertEquals(7, r.current("a"));
    assertEquals(7, r.current("b"));
    assertEquals(7, r.current("c"));
  }

  // -------- LatestAny mode --------

  @Test
  void latestAnyWakesOnAnyStreamAndSnapshotsFreshest() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.LATEST_ANY);

    // Only A has data: tick wakes on A alone; B stays null (never produced).
    wa.write(10);
    assertEquals(10L, r.tick());
    assertEquals(10, r.current("a"));
    assertNull(r.current("b"));

    // B advances; A unchanged. Row keeps A's stale value, B becomes fresh; rowT = max(10, 20) = 20.
    wb.write(20);
    assertEquals(20L, r.tick());
    assertEquals(10, r.current("a")); // stale
    assertEquals(20, r.current("b"));
  }

  @Test
  void latestAnyDrainsStreamToMostRecentItem() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(3, AionMultiQueueReader.Mode.LATEST_ANY);

    // A has a backlog of three items; one tick must drain to the freshest (30).
    wa.write(10);
    wa.write(20);
    wa.write(30);
    assertEquals(30L, r.tick());
    assertEquals(30, r.current("a"));
    assertEquals(20, r.get("a", 1)); // drained items land in the lookback window
    assertNull(r.current("b"));
  }

  @Test
  void latestAnyResolvedTimestampIsMonotonic() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.LATEST_ANY);

    wa.write(100);
    assertEquals(100L, r.tick());
    // B produces an OLDER timestamp than A's last; rowT stays at the newest across the row (100).
    wb.write(50);
    assertEquals(100L, r.tick());
    assertEquals(100, r.current("a"));
    assertEquals(50, r.current("b"));
  }

  @Test
  void latestAnyBlocksUntilAnyStreamProduces() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.LATEST_ANY);

    // Nothing buffered yet; tick must park until some stream writes.
    AtomicBoolean done = new AtomicBoolean(false);
    AtomicLong result = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    result.set(r.tick());
                    done.set(true);
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(100);
    assertFalse(done.get(), "latest-any tick should park until any stream produces");

    wb.write(7);
    t.join(2000);
    assertEquals(7L, result.get());
    assertEquals(7, r.current("b"));
  }

  @Test
  void latestAnyThrowsEosOnlyWhenAllStreamsExhausted() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.LATEST_ANY);

    wa.write(10);
    wa.close();
    // A is drained+closed but B is still open: not yet EOS, tick parks until B acts.
    assertEquals(10L, r.tick());

    wb.close(); // now every stream is exhausted.
    assertThrows(AionEndOfStreamException.class, r::tick);
  }

  @Test
  void latestAnyWakesMultipleReadersUnderConcurrentWriters() throws Exception {
    // Exercises the lock-free wake path at the intended scale: many writers fanning into many
    // independent LATEST_ANY readers. Every reader must observe every stream's final value and none
    // may hang — a lost wakeup would leave a reader parked forever and fail the join.
    int streams = 4;
    int perStream = 300;
    int readerCount = 3;
    AionMultiQueue<Integer> mq = newMQ();

    List<AionQueueWriter<Integer>> ws = new ArrayList<>();
    for (int s = 0; s < streams; s++) ws.add(mq.createStream("s" + s));

    List<AionMultiQueueReader<Integer>> rs = new ArrayList<>();
    for (int i = 0; i < readerCount; i++) {
      rs.add(mq.createReader(0, AionMultiQueueReader.Mode.LATEST_ANY));
    }

    List<Map<String, Integer>> finalRows = new ArrayList<>();
    for (int i = 0; i < readerCount; i++) finalRows.add(null);
    AtomicReference<Throwable> failure = new AtomicReference<>();

    List<Thread> readerThreads = new ArrayList<>();
    for (int i = 0; i < readerCount; i++) {
      final int idx = i;
      final AionMultiQueueReader<Integer> r = rs.get(i);
      readerThreads.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      while (true) {
                        try {
                          r.tick();
                        } catch (AionEndOfStreamException eos) {
                          break; // every stream drained and closed
                        }
                      }
                      finalRows.set(idx, r.currentRow());
                    } catch (Throwable t) {
                      failure.set(t);
                    }
                  }));
    }

    List<Thread> writerThreads = new ArrayList<>();
    for (int s = 0; s < streams; s++) {
      final AionQueueWriter<Integer> w = ws.get(s);
      writerThreads.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      for (int v = 1; v <= perStream; v++) w.write(v); // value == timestamp
                      w.close();
                    } catch (Throwable t) {
                      failure.set(t);
                    }
                  }));
    }

    for (Thread t : writerThreads) t.join(10_000);
    for (Thread t : readerThreads) t.join(10_000);

    assertNull(failure.get());
    for (int i = 0; i < readerCount; i++) {
      Map<String, Integer> row = finalRows.get(i);
      assertNotNull(row, "reader " + i + " never reached end-of-stream (possible lost wakeup)");
      for (int s = 0; s < streams; s++) {
        assertEquals(perStream, row.get("s" + s), "reader " + i + " missed tail of s" + s);
      }
    }
  }

  @Test
  void latestAnyWithTimeoutThrowsWhenAllStreamsSilent() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.LATEST_ANY);
    assertThrows(AionQueueReadException.class, () -> r.tick(Duration.ofMillis(80)));
  }

  @Test
  void defaultCreateReaderUsesStrictAligned() {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    AionMultiQueueReader<Integer> r = mq.createReader(0);
    assertEquals(AionMultiQueueReader.Mode.STRICT_ALIGNED, r.mode());
  }

  // -------- Timeouts on multi-reader tick --------

  @Test
  void strictTickWithTimeoutThrowsWhenAStreamIsSilent() {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    mq.createStream("b"); // B never writes
    AionMultiQueueReader<Integer> r = mq.createReader(0);
    try {
      wa.write(1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertThrows(AionQueueReadException.class, () -> r.tick(Duration.ofMillis(80)));
  }

  @Test
  void mergeTickWithTimeoutThrowsWhenAStreamIsSilent() {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0, AionMultiQueueReader.Mode.TIME_MERGED);
    try {
      wa.write(1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertThrows(AionQueueReadException.class, () -> r.tick(Duration.ofMillis(80)));
  }

  @Test
  void timeoutAppliesToWholeMultiTickNotPerSubReader() throws Exception {
    // Strict alignment: A writes 1, then B writes 1 at +60ms. Multi-tick with 200ms timeout
    // succeeds because the total wait (60ms) is within budget.
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);
    wa.write(1);
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                Thread.sleep(60);
                wb.write(1);
              } catch (Exception ignored) {
              }
            });
    assertEquals(1L, r.tick(Duration.ofMillis(300)));
  }

  @Test
  void strictTickWithTimeoutReturnsWhenAligned() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    AionQueueWriter<Integer> wa = mq.createStream("a");
    AionQueueWriter<Integer> wb = mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);
    wa.write(5);
    wb.write(5);
    assertEquals(5L, r.tick(Duration.ofSeconds(2)));
  }

  // -------- Bounded multi-queue --------

  @Test
  void defaultCapacityAppliesToCreatedStreams() throws Exception {
    AionMultiQueue<Integer> mq = new AionMultiQueue<>(INT_TS, 2);
    AionQueueWriter<Integer> w = mq.createStream("a");
    mq.createReader(0); // pins everything
    w.write(1);
    w.write(2);
    assertThrows(AionQueueWriteException.class, () -> w.write(3, Duration.ofMillis(80)));
  }

  @Test
  void perStreamCapacityOverridesDefault() throws Exception {
    AionMultiQueue<Integer> mq = new AionMultiQueue<>(INT_TS, 2);
    AionQueueWriter<Integer> bigW = mq.createStream("big", 100);
    mq.createReader(0);
    for (int i = 1; i <= 50; i++) bigW.write(i); // would block on a cap-2 stream
  }

  @Test
  void invalidDefaultCapacityRejected() {
    assertThrows(IllegalArgumentException.class, () -> new AionMultiQueue<>(INT_TS, 0));
    assertThrows(IllegalArgumentException.class, () -> new AionMultiQueue<>(INT_TS, 1));
  }

  // -------- close --------

  @Test
  void closeClosesEverySubReader() throws Exception {
    AionMultiQueue<Integer> mq = newMQ();
    mq.createStream("a");
    mq.createStream("b");
    AionMultiQueueReader<Integer> r = mq.createReader(0);

    AionQueueReader<Integer> ra = r.reader("a");
    AionQueueReader<Integer> rb = r.reader("b");
    r.close();
    AtomicReference<Throwable> e1 = new AtomicReference<>();
    AtomicReference<Throwable> e2 = new AtomicReference<>();
    try {
      ra.tick();
    } catch (Throwable t) {
      e1.set(t);
    }
    try {
      rb.tick();
    } catch (Throwable t) {
      e2.set(t);
    }
    assertInstanceOf(AionEndOfStreamException.class, e1.get());
    assertInstanceOf(AionEndOfStreamException.class, e2.get());
  }
}
