package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {

    private PoolConfig config;
    private final ConnectionFactory factory;
    private final ReentrantLock lock;
    private final Condition schedule;
    private final Condition returned;
    private final Condition closed;
    private final ArrayDeque<Idle> idle = new ArrayDeque<>();
    private final ArrayDeque<Waiter> waiting = new ArrayDeque<>();
    private final Set<Lease> borrowed = new HashSet<>();
    private final Thread expirationThread;
    private int creating;
    private int closing;
    private boolean stopped;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        lock = new ReentrantLock(config.fair());
        schedule = lock.newCondition();
        returned = lock.newCondition();
        closed = lock.newCondition();
        expirationThread = new Thread(this::expireLoop, "pool-expiration");
        expirationThread.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        validateDuration(timeout, "timeout");
        Waiter waiter = new Waiter(new Deadline(timeout));
        RawConnection toClose = null;
        lock.lock();
        try {
            ensureRunning();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (waiting.isEmpty()) {
                assign(waiter);
            }
            if (waiter.lease == null && !waiter.creating) {
                waiting.addLast(waiter);
            }
            while (waiter.lease == null && !waiter.creating) {
                ensureRunning();
                Duration left = waiter.deadline.remaining();
                if (left.isZero()) {
                    throw new PoolTimeoutException(timeout);
                }
                waiter.ready.awaitNanos(nanos(left));
            }
            ensureRunning();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (waiter.lease != null) {
                return waiter.lease;
            }
        } catch (InterruptedException | PoolTimeoutException | IllegalStateException e) {
            waiting.remove(waiter);
            if (waiter.creating) {
                creating--;
            }
            if (waiter.lease != null) {
                toClose = release(waiter.lease);
            }
            dispatch();
            throw e;
        } finally {
            lock.unlock();
            closeRaw(toClose);
        }
        return createConnection();
    }

    @Override
    public void resize(int maxSize) {
        List<RawConnection> toClose = new ArrayList<>();
        lock.lock();
        try {
            if (maxSize < 1 || maxSize > PoolConfig.MAX_POOL_SIZE || maxSize < config.minIdle()) {
                throw new IllegalArgumentException("Invalid maxSize");
            }
            ensureRunning();
            config = config.withMaxSize(maxSize);
            while (total() > maxSize && !idle.isEmpty()) {
                toClose.add(detach(idle.removeFirst().raw));
            }
            dispatch();
            schedule.signal();
        } finally {
            lock.unlock();
        }
        closeAll(toClose);
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try {
            return config;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(total(), idle.size(), borrowed.size(), creating, waiting.size());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        validateDuration(grace, "grace");
        Deadline deadline = new Deadline(grace);
        List<RawConnection> toClose = new ArrayList<>();
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            for (Waiter waiter : waiting) {
                waiter.ready.signal();
            }
            waiting.clear();
            while (!idle.isEmpty()) {
                toClose.add(detach(idle.removeFirst().raw));
            }
            schedule.signal();
        } finally {
            lock.unlock();
        }
        closeAll(toClose);
        toClose.clear();

        boolean interrupted = Thread.interrupted();
        lock.lock();
        try {
            while (!borrowed.isEmpty() && !interrupted) {
                Duration left = deadline.remaining();
                if (left.isZero()) {
                    break;
                }
                try {
                    returned.awaitNanos(nanos(left));
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            for (Lease lease : borrowed) {
                lease.open = false;
                toClose.add(detach(lease.raw));
            }
            borrowed.clear();
        } finally {
            lock.unlock();
        }
        closeAll(toClose);

        lock.lock();
        try {
            while (closing != 0) {
                try {
                    closed.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            lock.unlock();
        }
        while (true) {
            try {
                expirationThread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted || Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    private PooledConnection createConnection() throws ConnectionCreationException {
        RawConnection raw;
        try {
            raw = Objects.requireNonNull(factory.create(), "factory result");
        } catch (Exception e) {
            lock.lock();
            try {
                creating--;
                dispatch();
            } finally {
                lock.unlock();
            }
            throw new ConnectionCreationException(e);
        }
        lock.lock();
        try {
            creating--;
            if (!stopped) {
                Lease lease = new Lease(raw);
                borrowed.add(lease);
                return lease;
            }
            detach(raw);
        } finally {
            lock.unlock();
        }
        closeRaw(raw);
        throw new IllegalStateException("Pool is stopped");
    }

    private boolean assign(Waiter waiter) {
        if (!idle.isEmpty()) {
            waiter.lease = new Lease(idle.removeFirst().raw);
            borrowed.add(waiter.lease);
            schedule.signal();
            return true;
        }
        if (total() < config.maxSize()) {
            creating++;
            waiter.creating = true;
            return true;
        }
        return false;
    }

    private void dispatch() {
        while (!stopped && !waiting.isEmpty()) {
            Waiter waiter = waiting.getFirst();
            if (waiter.deadline.remaining().isZero() || assign(waiter)) {
                waiting.removeFirst();
                waiter.ready.signal();
            } else {
                break;
            }
        }
    }

    private RawConnection release(Lease lease) {
        if (!lease.open) {
            return null;
        }
        lease.open = false;
        borrowed.remove(lease);
        RawConnection toClose = null;
        if (stopped || total() >= config.maxSize()) {
            toClose = detach(lease.raw);
        } else {
            idle.addLast(new Idle(lease.raw, new Deadline(config.idleTimeout())));
        }
        dispatch();
        schedule.signal();
        if (borrowed.isEmpty()) {
            returned.signal();
        }
        return toClose;
    }

    private void expireLoop() {
        while (true) {
            List<RawConnection> toClose = new ArrayList<>();
            lock.lock();
            try {
                while (true) {
                    if (stopped) {
                        return;
                    }
                    if (idle.size() <= config.minIdle()) {
                        schedule.await();
                        continue;
                    }
                    Duration left = idle.getFirst().deadline.remaining();
                    if (left.isZero()) {
                        break;
                    }
                    schedule.awaitNanos(nanos(left));
                }
                while (idle.size() > config.minIdle()
                        && idle.getFirst().deadline.remaining().isZero()) {
                    toClose.add(detach(idle.removeFirst().raw));
                }
                dispatch();
            } catch (InterruptedException e) {
            } finally {
                lock.unlock();
            }
            closeAll(toClose);
        }
    }

    private int total() {
        return idle.size() + borrowed.size() + creating;
    }

    private RawConnection detach(RawConnection raw) {
        closing++;
        return raw;
    }

    private void closeAll(List<RawConnection> connections) {
        for (RawConnection raw : connections) {
            closeRaw(raw);
        }
    }

    private void closeRaw(RawConnection raw) {
        if (raw == null) {
            return;
        }
        try {
            raw.close();
        } catch (Exception e) {
        } finally {
            lock.lock();
            try {
                closing--;
                if (closing == 0) {
                    closed.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Pool is stopped");
        }
    }

    private static void validateDuration(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private final class Lease implements PooledConnection {
        private final RawConnection raw;
        private boolean open = true;

        private Lease(RawConnection raw) {
            this.raw = raw;
        }

        @Override
        public RawConnection raw() {
            lock.lock();
            try {
                if (!open) {
                    throw new IllegalStateException("Connection is closed");
                }
                return raw;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            RawConnection toClose;
            lock.lock();
            try {
                toClose = release(this);
            } finally {
                lock.unlock();
            }
            closeRaw(toClose);
        }
    }

    private final class Waiter {
        private final Condition ready = lock.newCondition();
        private final Deadline deadline;
        private Lease lease;
        private boolean creating;

        private Waiter(Deadline deadline) {
            this.deadline = deadline;
        }
    }

    private static final class Deadline {
        private final Duration timeout;
        private final long started = System.nanoTime();

        private Deadline(Duration timeout) {
            this.timeout = timeout;
        }

        private Duration remaining() {
            Duration left = timeout.minusNanos(System.nanoTime() - started);
            return left.isNegative() ? Duration.ZERO : left;
        }
    }

    private record Idle(RawConnection raw, Deadline deadline) {
    }
}
