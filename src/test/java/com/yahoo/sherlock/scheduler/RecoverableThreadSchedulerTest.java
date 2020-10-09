/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Test for {@code RecoverableThreadScheduler}.
 */
@Slf4j
public class RecoverableThreadSchedulerTest {

    private RecoverableThreadScheduler recoverableThreadScheduler1;
    private RecoverableThreadScheduler recoverableThreadScheduler2;
    private static AtomicInteger runCount = new AtomicInteger();
    HelperHandler helperHandler;

    @BeforeMethod
    public void setUp() throws Exception {
        helperHandler = new HelperHandler();
        recoverableThreadScheduler2 = new RecoverableThreadScheduler(3, helperHandler);
        recoverableThreadScheduler1 = new RecoverableThreadScheduler(5);
        runCount.set(0);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        recoverableThreadScheduler1.shutdown();
        recoverableThreadScheduler2.shutdown();
    }

    @Test
    public void testScheduleAtFixedRate() throws InterruptedException {

        Assert.assertEquals(recoverableThreadScheduler1.getCorePoolSize(), 5);
        ScheduledFuture future1 = recoverableThreadScheduler1.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                log.info("running test ScheduleAtFixedRate() ...");
            }
        }, 0, 1, TimeUnit.SECONDS);
        Thread.sleep(2000);
        Assert.assertFalse(future1.isDone());
        future1.cancel(true);
        Assert.assertTrue(future1.isDone());
    }

    @Data
    private static class HelperHandler implements ScheduledExceptionHandler {
        private AtomicInteger resubmitCount = new AtomicInteger(0);
        public boolean exceptionOccurred(Throwable e) {
            if (resubmitCount.get() >= 3) {
                return false;
            }
            resubmitCount.incrementAndGet();
            return true;
        }
    }

    private class TestRunnable implements Runnable {
        @Override
        public void run() {
            if (runCount.get() > 0) {
                runCount.incrementAndGet();
                throw new IllegalArgumentException("error");
            }
            runCount.incrementAndGet();
        }
    }

    @Test
    public void testAfterExecute() throws InterruptedException {
        Assert.assertEquals(recoverableThreadScheduler2.getCorePoolSize(), 3);
        ScheduledFuture future = recoverableThreadScheduler2.scheduleAtFixedRate(new TestRunnable(), 0, 1, TimeUnit.SECONDS);
        Thread.sleep(10000);
        Assert.assertEquals(helperHandler.getResubmitCount().get(), 3);
        Assert.assertEquals(runCount.get(), 5);
        future.cancel(true);
    }
}
