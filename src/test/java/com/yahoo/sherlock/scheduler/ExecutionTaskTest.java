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
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JobScheduler;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;

public class ExecutionTaskTest {

    @Test
    public void testExecutionTaskConsumeAndExecute() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
                                                            SchedulerException {
        JobExecutionService jes = Mockito.mock(JobExecutionService.class);
        JobScheduler js = Mockito.mock(JobScheduler.class);
        SchedulerService ss = Mockito.mock(SchedulerService.class);
        JobMetadataAccessor jma = Mockito.mock(JobMetadataAccessor.class);
        ExecutionTask et = new ExecutionTask(jes, ss, js, jma);
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

}
