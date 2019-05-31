/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.utils.TimeUtils;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZonedDateTime;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ExecutionTaskTest {

    private static void inject(ExecutionTask et, String n, Object v) {
        try {
            Field f = ExecutionTask.class.getDeclaredField(n);
            f.setAccessible(true);
            f.set(et, v);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail(e.toString());
        }
    }

    @Test
    public void testExecutionTaskConsumeAndExecute() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
                                                            SchedulerException {
        JobExecutionService jes = Mockito.mock(JobExecutionService.class);
        JobScheduler js = Mockito.mock(JobScheduler.class);
        SchedulerService ss = Mockito.mock(SchedulerService.class);
        JobMetadataAccessor jma = Mockito.mock(JobMetadataAccessor.class);
        EmailService ems = Mockito.mock(EmailService.class);
        ExecutionTask et = new ExecutionTask(jes, ss, js, jma);
        inject(et, "emailService", ems);
        JobMetadata jm = new JobMetadata();
        Integer[] idPtr = new Integer[] {5};
        jm.setJobId(idPtr[0]);
        jm.setEffectiveRunTime(12340);
        jm.setGranularity(Granularity.HOUR.toString());
        jm.setFrequency(Granularity.HOUR.toString());
        jm.setJobStatus(JobStatus.RUNNING.getValue());
        Mockito.when(js.popQueue(anyLong())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock iom) throws Throwable {
                Mockito.doAnswer(this).when(js).popQueue(anyLong());
                idPtr[0]--;
                jm.setJobId(idPtr[0]);
                if (idPtr[0] <= 0) {
                    return null;
                } else {
                    return jm;
                }
            }
        });
        Mockito.doNothing().when(ems).sendConsolidatedEmail(any(), any());
        Method m = et.getClass().getDeclaredMethod("consumeAndExecuteTasks", long.class);
        m.setAccessible(true);
        m.invoke(et, 12345);
        Mockito.verify(js, Mockito.times(5)).popQueue(12345);
        Mockito.verify(jes, Mockito.times(4)).execute(any(JobMetadata.class));
        Mockito.verify(ss, Mockito.times(4)).rescheduleJob(any(JobMetadata.class));
        Mockito.verify(jma, Mockito.times(4)).putJobMetadata(any(JobMetadata.class));
    }

    @Test
    public void testRunException() throws IOException, SchedulerException {
        JobExecutionService jes = Mockito.mock(JobExecutionService.class);
        JobScheduler js = Mockito.mock(JobScheduler.class);
        SchedulerService ss = Mockito.mock(SchedulerService.class);
        JobMetadataAccessor jma = Mockito.mock(JobMetadataAccessor.class);
        ExecutionTask et = new ExecutionTask(jes, ss, js, jma);
        Mockito.when(js.popQueue(anyLong())).thenThrow(new IOException());
        et.run();
        Mockito.verify(jes, Mockito.times(0)).execute(any(JobMetadata.class));
        Mockito.verify(ss, Mockito.times(0)).rescheduleJob(any(JobMetadata.class));
        Mockito.verify(js, Mockito.times(1)).popQueue(anyLong());
        Mockito.verify(jma, Mockito.times(0)).putJobMetadata(any(JobMetadata.class));
    }

    @Test
    public void testRunEmailSender() throws IOException {
        JobExecutionService jes = Mockito.mock(JobExecutionService.class);
        JobScheduler js = Mockito.mock(JobScheduler.class);
        SchedulerService ss = Mockito.mock(SchedulerService.class);
        JobMetadataAccessor jma = Mockito.mock(JobMetadataAccessor.class);
        EmailService ems = Mockito.mock(EmailService.class);
        ExecutionTask et = new ExecutionTask(jes, ss, js, jma);
        inject(et, "emailService", ems);
        Mockito.doNothing().when(ems).sendConsolidatedEmail(any(), any());
        long time = 33 * 24 * 60L + 13 * 60L + 12L;
        ZonedDateTime zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        assertEquals(zonedTime.getDayOfMonth(), 3);
        assertEquals(zonedTime.getDayOfWeek().getValue(), 2);
        // test without first day of month/first day of week(monday)
        et.runEmailSender(time);
        Mockito.verify(ems, Mockito.times(2)).sendConsolidatedEmail(any(), any());
        // test with first day of month
        time = time - 2 * 24 * 60L;
        zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        assertEquals(zonedTime.getDayOfMonth(), 1);
        et.runEmailSender(time);
        Mockito.verify(ems, Mockito.times(5)).sendConsolidatedEmail(any(), any());
        // test with first day of week
        time = time + 1 * 24 * 60L;
        zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        assertEquals(zonedTime.getDayOfWeek().getValue(), 1);
        et.runEmailSender(time);
        Mockito.verify(ems, Mockito.times(8)).sendConsolidatedEmail(any(), any());
    }

}
