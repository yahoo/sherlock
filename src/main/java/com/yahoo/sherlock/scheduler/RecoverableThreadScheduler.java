package com.yahoo.sherlock.scheduler;

import java.util.IdentityHashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Recoverable scheduled thread pool implementation.
 * If the scheduled thread dies due to exception at runtime, {@code RecoverableThreadScheduler}
 * resubmits the same runnable with given scheduling params or behaves according to the custom
 * implementation of {@code ScheduledExceptionHandler} if provided.
 */
@Slf4j
public class RecoverableThreadScheduler extends ScheduledThreadPoolExecutor {

    /** Default exception handler, always reschedules. */
    private static final ScheduledExceptionHandler NULL_HANDLER = e -> true;

    /** Map to keep track of all runnables for thread pool schedular. */
    private final Map<Object, SchedulerParams> runnables = new IdentityHashMap<>();

    /** Exception handler for runnables. */
    private final ScheduledExceptionHandler handler;

    /**
     * Constructor with poolsize param.
     * @param poolSize the number of threads to keep in the pool
     */
    public RecoverableThreadScheduler(int poolSize) {
        this(poolSize, NULL_HANDLER);
    }

    /**
     * Constructor with poolsize param and custom {@code ScheduledExceptionHandler} implementation.
     * @param poolSize the number of threads to keep in the pool
     * @param handler {@code ScheduledExceptionHandler} object
     */
    public RecoverableThreadScheduler(int poolSize, ScheduledExceptionHandler handler) {
        super(poolSize);
        this.handler = handler;
    }

    /**
     * Class to hold scheduling details about runnables.
     */
    @AllArgsConstructor
    private class SchedulerParams {
        private Runnable runnable;
        private long period;
        private TimeUnit unit;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long initialDelay, long period, TimeUnit unit) {
        ScheduledFuture<?> future = super.scheduleAtFixedRate(runnable, initialDelay, period, unit);
        runnables.put(future, new SchedulerParams(runnable, period, unit));
        return future;
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
        ScheduledFuture future = (ScheduledFuture) runnable;
        if (future.isDone()) {
            try {
                future.get();
                log.info("Task is completed");
            } catch (CancellationException ce) {
                log.error("Task is cancelled!");
            } catch (ExecutionException e) {
                log.error("Task is completed with exception!");
                Throwable t = e.getCause();
                SchedulerParams schedulerParams = runnables.remove(runnable);
                if (t != null && schedulerParams != null) {
                    boolean resubmit = handler.exceptionOccurred(t);
                    if (resubmit) {
                        log.info("Resubmitting the runnable task");
                        scheduleAtFixedRate(schedulerParams.runnable, schedulerParams.period, schedulerParams.period, schedulerParams.unit);
                    }
                }
            } catch (InterruptedException e) {
                log.error("Scheduler thread is interrupted!");
                Thread.currentThread().interrupt();
            }
        }
    }
}
