/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.utils.TimeUtils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
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
        JobMetadata jobMetadata = mock(JobMetadata.class);
        when(jobMetadata.getJobStatus()).thenReturn("RUNNING");
        ss.rescheduleJob(jobMetadata);
        Mockito.verify(ss, Mockito.times(1)).jobRescheduleTime(any());
        Mockito.verify(js, Mockito.times(1)).pushQueue(anyLong(), anyString());
        IOException ioex = new IOException("error");
        Mockito.doThrow(ioex).when(js).pushQueue(anyLong(), anyString());
        try {
            ss.rescheduleJob(jobMetadata);
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

    @Test
    public void testBulkStopJob() throws SchedulerException, IOException, JobNotFoundException {
        init();
        doCallRealMethod().when(ss).stopJob(anySet());
        Set<String> jobSet = new HashSet<String>() {
            {
                add("1");
                add("2");
                add("3");
            }
        };
        ss.stopJob(jobSet);
        Mockito.verify(js, Mockito.times(1)).removeQueue(jobSet);
        IOException ioex = new IOException("error");
        Mockito.doThrow(ioex).when(js).removeQueue(anySet());
        try {
            ss.stopJob(jobSet);
        } catch (SchedulerException e) {
            Assert.assertEquals(e.getMessage(), "error");
            Assert.assertEquals(e.getCause(), ioex);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testRemoveAllFromQueue() throws SchedulerException, IOException {
        init();
        doCallRealMethod().when(ss).removeAllJobsFromQueue();
        ss.removeAllJobsFromQueue();
        Mockito.verify(js, Mockito.times(1)).removeAllQueue();
        IOException ioex = new IOException("error");
        Mockito.doThrow(ioex).when(js).removeAllQueue();
        try {
            ss.removeAllJobsFromQueue();
        } catch (SchedulerException e) {
            Assert.assertEquals(e.getMessage(), "error");
            Assert.assertEquals(e.getCause(), ioex);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testJobScheduleTimeAndRescheduleTime() {
        init();
        int hoursOfLag = 36;
        JobMetadata jobMetadata = new JobMetadata();
        jobMetadata.setJobId(1);
        jobMetadata.setGranularity("day");
        jobMetadata.setFrequency("month");
        jobMetadata.setHoursOfLag(hoursOfLag);
        doCallRealMethod().when(ss).jobScheduleTime(jobMetadata);
        Pair<Integer, Integer> imp = ss.jobScheduleTime(jobMetadata);
        int expectedQueryTime = Granularity.MONTH.getEndTimeForInterval(ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag));
        Assert.assertEquals(imp.getLeft(), (Integer) expectedQueryTime);
        int expectedRunTime = expectedQueryTime + hoursOfLag * 60 + Math.abs(jobMetadata.getJobId()) % Constants.MINUTES_IN_HOUR;
        Assert.assertEquals(imp.getRight(), (Integer) expectedRunTime);
        jobMetadata.setEffectiveQueryTime(expectedQueryTime);
        jobMetadata.setEffectiveRunTime(expectedRunTime);
        doCallRealMethod().when(ss).jobRescheduleTime(jobMetadata);
        imp = ss.jobRescheduleTime(jobMetadata);
        Assert.assertEquals(imp.getLeft(), TimeUtils.addMonth(expectedQueryTime, 1));
        Assert.assertEquals(imp.getRight(), TimeUtils.addMonth(expectedRunTime, 1));
        jobMetadata.setFrequency("day");
        imp = ss.jobRescheduleTime(jobMetadata);
        Assert.assertEquals(imp.getLeft(), (Integer) (expectedQueryTime + Granularity.DAY.getMinutes()));
        Assert.assertEquals(imp.getRight(), (Integer) (expectedRunTime + Granularity.DAY.getMinutes()));
        // for minute frequency jobs
        hoursOfLag = 0;
        jobMetadata.setGranularity("minute");
        jobMetadata.setFrequency("minute");
        jobMetadata.setHoursOfLag(hoursOfLag);
        imp = ss.jobScheduleTime(jobMetadata);
        expectedQueryTime = Granularity.MINUTE.getEndTimeForInterval(ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag));
        Assert.assertEquals(imp.getLeft(), (Integer) expectedQueryTime);
        expectedRunTime = expectedQueryTime + hoursOfLag * 60 + 1;
        Assert.assertEquals(imp.getRight(), (Integer) expectedRunTime);
    }
}
