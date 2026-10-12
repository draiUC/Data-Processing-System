package dps;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe FIFO task queue guarded by one ReentrantLock.
 * Workers block on a Condition (no busy-waiting) until work arrives or the queue is closed.
 */
public class SharedTaskQueue {
    private final Deque<Task> tasks = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private boolean closed = false;

    /** Adds a task and wakes one waiting worker. */
    public void addTask(Task task) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("queue is closed; task " + task.id() + " rejected");
            }
            tasks.addLast(task);
            notEmpty.signal();
        } finally {
            lock.unlock(); // always released, even if an exception is thrown
        }
    }

    /**
     * Removes the next task, waiting up to the timeout.
     *
     * @return the task, or Optional.empty() once the queue is closed AND fully drained
     *         (the shutdown signal for workers)
     * @throws EmptyQueueException  the queue stayed empty for the whole timeout but is still open
     * @throws InterruptedException the waiting thread was interrupted
     */
    public Optional<Task> getTask(long timeout, TimeUnit unit)
            throws InterruptedException, EmptyQueueException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (tasks.isEmpty()) {
                if (closed) {
                    return Optional.empty();
                }
                if (nanos <= 0L) {
                    throw new EmptyQueueException("no task arrived within " + timeout + " " + unit);
                }
                nanos = notEmpty.awaitNanos(nanos); // releases the lock while waiting
            }
            return Optional.of(tasks.removeFirst());
        } finally {
            lock.unlock();
        }
    }

    /** No more tasks will be added; wakes every waiting worker so it can drain and exit. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
