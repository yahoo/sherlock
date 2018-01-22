/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.store.JobScheduler;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for Scheduler service.
 */
public class SchedulerServiceTest {

    private static void inject(SchedulerService ss, Object b) {
        try {
            Field f = SchedulerService.class.getDeclaredField("jobScheduler");
            f.setAccessible(true);
            f.set(ss, b);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail();
        }
    }

    private JobScheduler js;
    private SchedulerService ss;

    private void init() {
        js = mock(JobScheduler.class);
        ss = mock(SchedulerService.class);
        inject(ss, js);
    }

    @Test
    public void testScheduleJob() throws SchedulerException, IOException {
        init();
        when(ss.jobScheduleTime(any())).thenReturn(new ImmutablePair<>(5, 5));
        doCallRealMethod().when(ss).scheduleJob(any());
        ss.scheduleJob(mock(JobMetadata.class));
        Mockito.verify(ss, Mockito.times(1)).jobScheduleTime(any());
        Mockito.verify(js, Mockito.times(1)).pushQueue(anyLong(), anyString());
        IOException ioex = new IOException("error");
        Mockito.doThrow(ioex).when(js).pushQueue(anyLong(), anyString());
        try {
            ss.scheduleJob(mock(JobMetadata.class));
        } catch (SchedulerException e) {
            Assert.assertEquals(e.getMessage(), "error");
            Assert.assertEquals(e.getCause(), ioex);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testRescheduleJob() throws SchedulerException, IOException {
        init();
        doCallRealMethod().when(ss).rescheduleJob(any());
        when(ss.jobRescheduleTime(any())).thenReturn(new ImmutablePair<>(5, 5));
        ss.rescheduleJob(mock(JobMetadata.class));
        Mockito.verify(ss, Mockito.times(1)).jobRescheduleTime(any());
        Mockito.verify(js, Mockito.times(1)).pushQueue(anyLong(), anyString());
        IOException ioex = new IOException("error");
        Mockito.doThrow(ioex).when(js).pushQueue(anyLong(), anyString());
        try {
            ss.rescheduleJob(mock(JobMetadata.class));
        } catch (SchedulerException e) {
            Assert.assertEquals(e.getMessage(), "error");
            Assert.assertEquals(e.getCause(), ioex);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testStopJob() throws SchedulerException, IOException, JobNotFoundException {
        init();
        doCallRealMethod().when(ss).stopJob(anyInt());
        ss.stopJob(5);
        Mockito.verify(js, Mockito.times(1)).removeQueue(5);
        IOException ioex = new IOException("error");
        Mockito.doThrow(ioex).when(js).removeQueue(anyInt());
        try {
            ss.stopJob(5);
        } catch (SchedulerException e) {
            Assert.assertEquals(e.getMessage(), "error");
            Assert.assertEquals(e.getCause(), ioex);
            return;
        }
        Assert.fail();
    }

}
