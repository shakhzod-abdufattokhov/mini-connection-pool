package com.nemo.connection_pool_manager;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
final class LeakTask implements Runnable{

    private final Exception borrowedAt;
    private final String threadName;
    private final long thresholdMillis;
    private final AtomicInteger leakCounter;
    private volatile ScheduledFuture<?> future;
    private volatile boolean leaked;

    private LeakTask(long thresholdMillis, AtomicInteger leakCounter) {
        this.borrowedAt = new Exception("Apparent connection leaked detected");
        this.threadName = Thread.currentThread().getName();
        this.thresholdMillis = thresholdMillis;
        this.leakCounter = leakCounter;
    }

    static LeakTask schedule(ScheduledExecutorService executorService, long threasholdMills, AtomicInteger leakCounter) {
        if(threasholdMills <= 0) {
            return null;
        }

        LeakTask task = new LeakTask(threasholdMills, leakCounter);
        task.future = executorService.schedule(task, threasholdMills, TimeUnit.MILLISECONDS);
        return task;
    }

    @Override
    public void run() {
        leaked = true;
        leakCounter.incrementAndGet();
        log.warn("Connection held by thread '{}' for more than {} ms. Borrowed at:",
                threadName, thresholdMillis, borrowedAt);
    }

    void cancel() {
        ScheduledFuture<?> f = future;
        if(f != null) f.cancel(false);
        if(leaked) {
            log.info("Connection from thread '{}' was returned after a leak warning (slow query, not a true leak)", threadName);
        }
    }
}
