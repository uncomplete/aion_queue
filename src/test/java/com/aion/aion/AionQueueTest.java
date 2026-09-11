package com.aion.aion;

import static org.junit.jupiter.api.Assertions.*;

import com.aion.aion.error.AionEndOfStreamException;
import com.aion.aion.error.AionQueueMonotonicityException;
import com.aion.aion.error.AionQueueReadException;
import com.aion.aion.error.AionQueueWriteException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AionQueueTest {

  // For Integer items we use the value itself as the timestamp.
  private static final Function<Integer, Long> INT_TS = i -> (long) i;

  private static AionQueue<Integer> newQueue() {
    return new AionQueue<>(INT_TS);
  }

  // -------- Construction / single-writer guard --------

  @Test
  void duplicateWriterRejected() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    assertThrows(IllegalStateException.class, q::openWriter);
  }

  @Test
  void multipleReadersAllowed() {
    AionQueue<Integer> q = newQueue();
    assertNotNull(q.openReader(0));
    assertNotNull(q.openReader(0));
    assertNotNull(q.openReader(3));
  }

  @Test
  void negativeLookbackCapacityRejected() {
    AionQueue<Integer> q = newQueue();
    assertThrows(IllegalArgumentException.class, () -> q.openReader(-1));
  }

  @Test
  void nullExtractorRejected() {
    assertThrows(IllegalArgumentException.class, () -> new AionQueue<Integer>(null));
  }

  // -------- FIFO ordering by timestamp --------

  @Test
  void tickReturnsTimestampsInOrder() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(2);
    w.write(3);
    assertEquals(1L, r.tick());
    assertEquals(1, r.current());
    assertEquals(2L, r.tick());
    assertEquals(3L, r.tick());
  }

  @Test
  void writerEnforcesStrictMonotonicity() {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    assertDoesNotThrow(() -> w.write(5));
    assertThrows(AionQueueMonotonicityException.class, () -> w.write(5));
    assertThrows(AionQueueMonotonicityException.class, () -> w.write(4));
  }

  // -------- Broadcast: all readers see all items --------

  @Test
  void everyReaderSeesEveryItem() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r1 = q.openReader(0);
    AionQueueReader<Integer> r2 = q.openReader(0);
    AionQueueReader<Integer> r3 = q.openReader(0);

    for (int i = 1; i <= 5; i++) w.write(i);

    for (AionQueueReader<Integer> r : List.of(r1, r2, r3)) {
      for (int i = 1; i <= 5; i++) {
        assertEquals((long) i, r.tick(), "reader missed item " + i);
      }
    }
  }

  @Test
  void readersAdvanceIndependently() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> fast = q.openReader(0);
    AionQueueReader<Integer> slow = q.openReader(0);

    for (int i = 1; i <= 4; i++) w.write(i);

    for (int i = 1; i <= 4; i++) assertEquals((long) i, fast.tick());
    for (int i = 1; i <= 4; i++) assertEquals((long) i, slow.tick());
  }

  @Test
  void newReaderSkipsItemsWrittenBeforeAttach() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> early = q.openReader(0);

    w.write(1);
    w.write(2);

    // Late reader attaches; it should not see ts<=2.
    AionQueueReader<Integer> late = q.openReader(0);
    w.write(3);
    w.write(4);
    w.close();

    assertEquals(1L, early.tick());
    assertEquals(2L, early.tick());
    assertEquals(3L, early.tick());
    assertEquals(4L, early.tick());
    assertThrows(AionEndOfStreamException.class, early::tick);

    assertEquals(3L, late.tick());
    assertEquals(4L, late.tick());
    assertThrows(AionEndOfStreamException.class, late::tick);
  }

  // -------- tickTo --------

  @Test
  void tickToSkipsPastIntermediateItems() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(5);
    w.write(10);
    assertEquals(10L, r.tickTo(7));
    assertEquals(10, r.current());
  }

  @Test
  void tickToReturnsExactTargetWhenPresent() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(5);
    w.write(10);
    assertEquals(5L, r.tickTo(5));
  }

  @Test
  void tickToWithTargetAtOrBelowCursorBehavesLikeTick() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(5);
    assertEquals(1L, r.tick());
    // cursor is 1; tickTo(0) clamps to cursor+1=2; smallest >= 2 is 5.
    assertEquals(5L, r.tickTo(0));
  }

  @Test
  void tickToBlocksUntilTargetAvailable() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);

    AtomicLong out = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    out.set(r.tickTo(10));
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(50);
    assertTrue(t.isAlive(), "tickTo(10) should be parked while only ts=1 exists");
    w.write(5); // does not satisfy target 10
    Thread.sleep(50);
    assertTrue(t.isAlive(), "ts=5 still does not satisfy target 10");
    w.write(12);
    t.join(2000);
    assertEquals(12L, out.get());
  }

  @Test
  void tickToThrowsEosWhenWriterClosedAndNoSatisfyingItem() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(5);
    w.close();
    assertEquals(1L, r.tick());
    assertEquals(5L, r.tick());
    assertThrows(AionEndOfStreamException.class, () -> r.tickTo(10));
  }

  // -------- Timeouts --------

  @Test
  void tickWithTimeoutThrowsWhenNoItemArrives() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    long start = System.nanoTime();
    assertThrows(AionQueueReadException.class, () -> r.tick(Duration.ofMillis(80)));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertTrue(elapsedMs >= 70, "expected ~80ms wait, got " + elapsedMs);
  }

  @Test
  void tickWithTimeoutReturnsImmediatelyWhenItemAvailable() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(42);
    assertEquals(42L, r.tick(Duration.ofSeconds(5)));
  }

  @Test
  void tickWithTimeoutUnblocksOnLateWrite() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);

    AtomicLong out = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    out.set(r.tick(Duration.ofSeconds(2)));
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(50);
    w.write(7);
    t.join(2000);
    assertEquals(7L, out.get());
  }

  @Test
  void tickToWithTimeoutThrowsWhenTargetUnreached() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    // ts=1 doesn't satisfy tickTo(100); times out.
    assertThrows(AionQueueReadException.class, () -> r.tickTo(100, Duration.ofMillis(80)));
  }

  @Test
  void peekNextTWithTimeoutThrowsWhenNoItemArrives() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    assertThrows(AionQueueReadException.class, () -> r.peekNextT(Duration.ofMillis(80)));
  }

  @Test
  void peekNextTWithTimeoutReturnsWithoutAdvancing() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(5);
    assertEquals(5L, r.peekNextT(Duration.ofSeconds(1)));
    // No advance — a subsequent tick still consumes ts=5.
    assertEquals(5L, r.tick());
  }

  @Test
  void zeroTimeoutThrowsWhenNoItemAvailable() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    assertThrows(AionQueueReadException.class, () -> r.tick(Duration.ZERO));
  }

  @Test
  void negativeTimeoutRejected() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    assertThrows(IllegalArgumentException.class, () -> r.tick(Duration.ofMillis(-1)));
  }

  // -------- Eviction --------

  @Test
  void slowestReaderPinsBuffer() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> fast = q.openReader(0);
    AionQueueReader<Integer> slow = q.openReader(0);

    for (int i = 1; i <= 5; i++) w.write(i); // ts 1..5
    for (int i = 1; i <= 5; i++) fast.tick(); // fast pins ts=5

    // slow pre-tick: floor = MIN_VALUE; nothing evicted.
    assertEquals(5, bufferSize(q));

    // Slow ticks twice: cursor 1→2; lookback=[value-at-2]; floor=2 → evicts ts=1.
    slow.tick();
    slow.tick();
    assertFalse(bufferContainsT(q, 1L));
    assertTrue(bufferContainsT(q, 2L));
  }

  @Test
  void closingSlowReaderReleasesPin() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> fast = q.openReader(0);
    AionQueueReader<Integer> slow = q.openReader(0);

    for (int i = 1; i <= 10; i++) w.write(i);
    for (int i = 1; i <= 10; i++) fast.tick();

    assertEquals(10, bufferSize(q)); // slow pins everything

    slow.close();
    // Only fast remains, pinning ts=10.
    assertEquals(1, bufferSize(q));
    assertTrue(bufferContainsT(q, 10L));
  }

  // -------- Blocking semantics --------

  @Test
  void tickBlocksThenWakesOnWrite() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);

    AtomicLong out = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    out.set(r.tick());
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(50);
    assertTrue(t.isAlive(), "reader should be parked");
    w.write(42);
    t.join(2000);
    assertEquals(42L, out.get());
  }

  @Test
  void writeWakesAllParkedReaders() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r1 = q.openReader(0);
    AionQueueReader<Integer> r2 = q.openReader(0);
    AionQueueReader<Integer> r3 = q.openReader(0);

    AtomicLong s1 = new AtomicLong(-1);
    AtomicLong s2 = new AtomicLong(-1);
    AtomicLong s3 = new AtomicLong(-1);
    CountDownLatch ready = new CountDownLatch(3);

    Thread t1 = tickThread(r1, s1, ready);
    Thread t2 = tickThread(r2, s2, ready);
    Thread t3 = tickThread(r3, s3, ready);

    ready.await();
    Thread.sleep(50);
    w.write(99);
    t1.join(2000);
    t2.join(2000);
    t3.join(2000);
    assertEquals(99L, s1.get());
    assertEquals(99L, s2.get());
    assertEquals(99L, s3.get());
  }

  @Test
  void tickThrowsEosAfterWriterClosesAndReaderDrains() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(2);
    w.close();
    assertEquals(1L, r.tick());
    assertEquals(2L, r.tick());
    assertThrows(AionEndOfStreamException.class, r::tick);
  }

  @Test
  void writerCloseUnblocksParkedReadersWithEos() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r1 = q.openReader(0);
    AionQueueReader<Integer> r2 = q.openReader(0);

    AtomicReference<Throwable> e1 = new AtomicReference<>();
    AtomicReference<Throwable> e2 = new AtomicReference<>();
    CountDownLatch ready = new CountDownLatch(2);
    Thread t1 = errThread(r1, e1, ready);
    Thread t2 = errThread(r2, e2, ready);
    ready.await();
    Thread.sleep(50);
    w.close();
    t1.join(2000);
    t2.join(2000);
    assertInstanceOf(AionEndOfStreamException.class, e1.get());
    assertInstanceOf(AionEndOfStreamException.class, e2.get());
  }

  @Test
  void closeUnblocksOnlyTheClosedReaderWithEos() throws Exception {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r1 = q.openReader(0);
    AionQueueReader<Integer> r2 = q.openReader(0);

    AtomicReference<Throwable> e1 = new AtomicReference<>();
    AtomicReference<Throwable> e2 = new AtomicReference<>();
    CountDownLatch ready = new CountDownLatch(2);
    Thread t1 = errThread(r1, e1, ready);
    Thread t2 = errThread(r2, e2, ready);
    ready.await();
    Thread.sleep(50);

    r1.close();
    t1.join(2000);
    assertInstanceOf(AionEndOfStreamException.class, e1.get());
    assertTrue(t2.isAlive(), "r2 should still be parked");
    t2.interrupt();
    t2.join(2000);
  }

  // -------- Lookback window --------

  @Test
  void getReturnsValueAtStepsBack() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(3);
    for (int i = 10; i <= 40; i += 10) w.write(i);
    for (int i = 0; i < 4; i++) r.tick();

    assertEquals(40, r.current());
    assertEquals(40, r.get(0));
    assertEquals(30, r.get(1));
    assertEquals(20, r.get(2));
    assertEquals(10, r.get(3));
    assertNull(r.get(4));
  }

  @Test
  void lookbackSlidesAsReaderAdvances() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(2);
    for (int i = 1; i <= 5; i++) w.write(i);
    for (int i = 0; i < 5; i++) r.tick();

    assertEquals(5, r.get(0));
    assertEquals(4, r.get(1));
    assertEquals(3, r.get(2));
    assertNull(r.get(3));
  }

  @Test
  void lookbackCapacityZeroRetainsOnlyCurrent() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(2);
    r.tick();
    r.tick();
    assertEquals(2, r.current());
    assertNull(r.get(1));
  }

  @Test
  void currentBeforeFirstTickIsNull() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r = q.openReader(3);
    assertNull(r.current());
    assertNull(r.get(0));
  }

  @Test
  void getWithNegativeStepsBackThrows() {
    AionQueue<Integer> q = newQueue();
    q.openWriter();
    AionQueueReader<Integer> r = q.openReader(3);
    assertThrows(IllegalArgumentException.class, () -> r.get(-1));
  }

  @Test
  void lookbackPinsBufferAcrossMultipleReaders() throws Exception {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> small = q.openReader(0); // pins 1 item back
    AionQueueReader<Integer> big = q.openReader(3); // pins 4 items back

    for (int i = 1; i <= 10; i++) w.write(i);
    for (int i = 0; i < 10; i++) {
      small.tick();
      big.tick();
    }
    // small floor = ts=10; big floor = ts=7; min = 7. Buffer keeps ts 7..10 = 4 entries.
    assertEquals(4, bufferSize(q));
    assertTrue(bufferContainsT(q, 7L));
    assertFalse(bufferContainsT(q, 6L));
  }

  // -------- Bounded queue / backpressure --------

  @Test
  void boundedQueueRejectsInvalidCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new AionQueue<>(INT_TS, 0));
    assertThrows(IllegalArgumentException.class, () -> new AionQueue<>(INT_TS, 1));
    assertThrows(IllegalArgumentException.class, () -> new AionQueue<>(INT_TS, -5));
  }

  @Test
  void openReaderRejectsLookbackThatWouldDeadlock() {
    // capacity=2 ⇒ only lookback=0 is admissible (0 + 2 <= 2).
    AionQueue<Integer> q = new AionQueue<>(INT_TS, 2);
    assertThrows(IllegalArgumentException.class, () -> q.openReader(1));
    // capacity=5 ⇒ lookback up to 3 is OK; 4 would need capacity >= 6.
    AionQueue<Integer> q2 = new AionQueue<>(INT_TS, 5);
    assertNotNull(q2.openReader(3));
    assertThrows(IllegalArgumentException.class, () -> q2.openReader(4));
  }

  @Test
  void writeBlocksWhenBufferAtCapacity() throws Exception {
    // capacity=3, reader lookback=0. Reader can pin 1 item; buffer can hold 3 items.
    // After 3 writes (no tick), writer blocks on the 4th.
    AionQueue<Integer> q = new AionQueue<>(INT_TS, 3);
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(2);
    w.write(3);

    AtomicBoolean done = new AtomicBoolean(false);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    w.write(4);
                    done.set(true);
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(50);
    assertFalse(done.get(), "writer should be parked on backpressure");

    // Reader advances; lookback rotates after the 2nd tick → frees ts=1; writer unblocks.
    r.tick();
    r.tick();
    t.join(2000);
    assertTrue(done.get());
  }

  @Test
  void writeWithTimeoutThrowsBackpressureWhenReaderNeverAdvances() throws Exception {
    AionQueue<Integer> q = new AionQueue<>(INT_TS, 2);
    AionQueueWriter<Integer> w = q.openWriter();
    q.openReader(0); // pins everything from now
    w.write(1);
    w.write(2);
    long start = System.nanoTime();
    AionQueueWriteException ex =
        assertThrows(AionQueueWriteException.class, () -> w.write(3, Duration.ofMillis(80)));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertTrue(elapsedMs >= 70, "expected ~80ms wait, got " + elapsedMs);
    assertEquals(2, ex.getBufferSize());
    assertEquals(2, ex.getCapacity());
  }

  @Test
  void writeWithTimeoutSucceedsWhenReaderAdvancesInTime() throws Exception {
    AionQueue<Integer> q = new AionQueue<>(INT_TS, 2);
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.write(1);
    w.write(2);

    Thread.ofVirtual()
        .start(
            () -> {
              try {
                Thread.sleep(50);
                r.tick();
                r.tick();
              } catch (Exception ignored) {
              }
            });
    // No throw means the write succeeded within the 500ms budget.
    w.write(3, Duration.ofMillis(500));
  }

  @Test
  void closingSlowReaderUnblocksWriterUnderBackpressure() throws Exception {
    AionQueue<Integer> q = new AionQueue<>(INT_TS, 2);
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> slow = q.openReader(0);
    w.write(1);
    w.write(2);

    AtomicBoolean done = new AtomicBoolean(false);
    CountDownLatch ready = new CountDownLatch(1);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    ready.countDown();
                    w.write(3);
                    done.set(true);
                  } catch (Exception ignored) {
                  }
                });
    ready.await();
    Thread.sleep(50);
    assertFalse(done.get());
    slow.close(); // removes the only reader; eviction frees the buffer
    t.join(2000);
    assertTrue(done.get());
  }

  @Test
  void unboundedQueueNeverBlocksWrites() throws Exception {
    AionQueue<Integer> q = newQueue(); // default unbounded
    AionQueueWriter<Integer> w = q.openWriter();
    q.openReader(0);
    for (int i = 1; i <= 10_000; i++) w.write(i);
    // No exception, no hang.
  }

  // -------- Writer close semantics --------

  @Test
  void writeAfterCloseThrows() {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    w.close();
    assertThrows(IllegalStateException.class, () -> w.write(1));
  }

  @Test
  void doubleCloseIsNoOp() {
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r = q.openReader(0);
    w.close();
    w.close();
    r.close();
    r.close();
  }

  // -------- Concurrent producer / multiple consumers --------

  @Test
  void concurrentProducerMultipleConsumersAllReceiveEverythingInOrder() throws Exception {
    final int N = 5_000;
    AionQueue<Integer> q = newQueue();
    AionQueueWriter<Integer> w = q.openWriter();
    AionQueueReader<Integer> r1 = q.openReader(0);
    AionQueueReader<Integer> r2 = q.openReader(0);
    AionQueueReader<Integer> r3 = q.openReader(0);

    AtomicReference<Throwable> err = new AtomicReference<>();
    List<Integer> seen1 = new ArrayList<>(N);
    List<Integer> seen2 = new ArrayList<>(N);
    List<Integer> seen3 = new ArrayList<>(N);

    Thread c1 = drainThread(r1, seen1, err);
    Thread c2 = drainThread(r2, seen2, err);
    Thread c3 = drainThread(r3, seen3, err);

    Thread producer =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    // writes ts=0..N-1 strictly monotonic
                    for (int i = 0; i < N; i++) w.write(i);
                    w.close();
                  } catch (Throwable t) {
                    err.set(t);
                  }
                });

    producer.join(5000);
    c1.join(5000);
    c2.join(5000);
    c3.join(5000);

    assertNull(err.get(), () -> "background error: " + err.get());
    for (List<Integer> seen : List.of(seen1, seen2, seen3)) {
      assertEquals(N, seen.size());
      for (int i = 0; i < N; i++) {
        assertEquals(i, seen.get(i), "order violated at index " + i);
      }
    }
  }

  // -------- helpers --------

  private static Thread tickThread(
      AionQueueReader<Integer> r, AtomicLong out, CountDownLatch ready) {
    return Thread.ofVirtual()
        .start(
            () -> {
              try {
                ready.countDown();
                out.set(r.tick());
              } catch (Exception ignored) {
              }
            });
  }

  private static Thread errThread(
      AionQueueReader<Integer> r, AtomicReference<Throwable> out, CountDownLatch ready) {
    return Thread.ofVirtual()
        .start(
            () -> {
              try {
                ready.countDown();
                r.tick();
              } catch (Throwable t) {
                out.set(t);
              }
            });
  }

  private static Thread drainThread(
      AionQueueReader<Integer> r, List<Integer> sink, AtomicReference<Throwable> err) {
    return Thread.ofVirtual()
        .start(
            () -> {
              try {
                while (true) {
                  r.tick();
                  sink.add(r.current());
                }
              } catch (AionEndOfStreamException eos) {
                // normal end
              } catch (Throwable t) {
                err.set(t);
              }
            });
  }

  private static int bufferSize(AionQueue<?> q) {
    q.lock.lock();
    try {
      return q.buffer.size();
    } finally {
      q.lock.unlock();
    }
  }

  private static boolean bufferContainsT(AionQueue<?> q, long t) {
    q.lock.lock();
    try {
      return q.buffer.containsKey(t);
    } finally {
      q.lock.unlock();
    }
  }
}
