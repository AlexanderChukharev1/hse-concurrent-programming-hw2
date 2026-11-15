package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(40)
class PoolStarvationTest {

    @Test
    void allRequestsWaitForSecondConnection() throws Exception {
        int threads = 3;
        AtomicInteger nextId = new AtomicInteger();
        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, threads, Duration.ofMinutes(5), true),
                () -> new TestConnection(nextId.incrementAndGet()));
        CyclicBarrier firstBorrowed = new CyclicBarrier(threads);
        CountDownLatch completed = new CountDownLatch(threads);
        Map<String, Integer> owners = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        List<Thread> workers = new ArrayList<>();
        try {
            for (int i = 1; i <= threads; i++) {
                Thread worker = new Thread(() -> {
                    try (PooledConnection first = pool.borrow(Duration.ofMinutes(1))) {
                        owners.put(Thread.currentThread().getName(), ((TestConnection) first.raw()).id);
                        firstBorrowed.await();
                        try (PooledConnection second = pool.borrow(Duration.ofMinutes(1))) {
                            second.raw();
                            completed.countDown();
                        }
                    } catch (InterruptedException expected) {
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                }, "starvation-request-" + i);
                workers.add(worker);
                worker.start();
            }
            awaitWaiting(pool, threads);
            assertEquals(new PoolStats(threads, 0, threads, 0, threads), pool.stats());
            assertEquals(threads, completed.getCount());
            assertTrue(failures.isEmpty(), failures.toString());
            System.out.println("Held connections: " + owners);
            assertFalse(completed.await(5, TimeUnit.SECONDS));
            assertEquals(threads, pool.stats().waiting());
            assertEquals(threads, completed.getCount());
        } finally {
            for (Thread worker : workers) {
                worker.interrupt();
            }
            for (Thread worker : workers) {
                worker.join(5000);
            }
            pool.shutdown(Duration.ZERO);
        }
        assertTrue(workers.stream().noneMatch(Thread::isAlive));
        assertTrue(failures.isEmpty(), failures.toString());
    }

    @Test
    void minimalPoolLetsEveryRequestFinish() throws Exception {
        int threads = 6;
        for (int connections = 1; connections <= 4; connections++) {
            int maxSize = threads * (connections - 1) + 1;
            ConnectionPool pool = new ConnectionPoolImpl(
                    new PoolConfig(0, maxSize, Duration.ofMinutes(5), true), () -> () -> {});
            CyclicBarrier partialBorrowed = new CyclicBarrier(threads);
            CountDownLatch completed = new CountDownLatch(threads);
            ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
            List<Thread> workers = new ArrayList<>();
            int needed = connections;
            try {
                for (int i = 0; i < threads; i++) {
                    Thread worker = new Thread(() -> {
                        List<PooledConnection> held = new ArrayList<>();
                        try {
                            for (int j = 0; j < needed - 1; j++) {
                                held.add(pool.borrow(Duration.ofSeconds(5)));
                            }
                            partialBorrowed.await(5, TimeUnit.SECONDS);
                            held.add(pool.borrow(Duration.ofSeconds(5)));
                        } catch (Throwable e) {
                            failures.add(e);
                        } finally {
                            held.forEach(PooledConnection::close);
                            completed.countDown();
                        }
                    }, "safe-request-" + i);
                    workers.add(worker);
                    worker.start();
                }
                assertTrue(completed.await(10, TimeUnit.SECONDS));
                assertTrue(failures.isEmpty(), failures.toString());
                assertEquals(0, pool.stats().borrowed());
            } finally {
                for (Thread worker : workers) {
                    worker.interrupt();
                    worker.join(5000);
                }
                pool.shutdown(Duration.ZERO);
            }
        }
    }

    private static void awaitWaiting(ConnectionPool pool, int expected) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (pool.stats().waiting() != expected && System.nanoTime() < limit) {
            Thread.sleep(1);
        }
        assertEquals(expected, pool.stats().waiting());
    }

    private record TestConnection(int id) implements RawConnection {
        @Override
        public void close() {
        }
    }
}
