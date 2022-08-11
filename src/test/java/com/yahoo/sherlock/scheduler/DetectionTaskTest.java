/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.beust.jcommander.internal.Lists;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.JobExecutionService;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.redis.LettuceAnomalyReportAccessor;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class DetectionTaskTest {

    @Test
    public void testDetectionTaskError() throws SherlockException {
        JobMetadata j = new JobMetadata();
        AnomalyReport ar = new AnomalyReport();
        ar.setStatus(Constants.ERROR);
        int runtime = 1234;
        List<TimeSeries> tslist = Collections.emptyList();
        DetectorService ds = mock(DetectorService.class);
        JobExecutionService jes = mock(JobExecutionService.class);
        DetectionTask et = new DetectionTask(j, runtime, tslist, ds, jes);
        when(ds.runDetection(any(), anyDouble(), any(), anyInt(), anyString(), any(), anyInt())).thenThrow(new SherlockException());
        when(jes.getSingletonReport(any())).thenReturn(ar);
        et.run();
        assertEquals(et.getReports().get(0).getStatus(), Constants.ERROR);
    }

    @Test
    public void testDetectionTaskRunToCompletion() throws SherlockException, IOException {
        JobMetadata j = new JobMetadata();
        j.setJobId(1);
        LettuceAnomalyReportAccessor ara = mock(LettuceAnomalyReportAccessor.class);
        j.setGranularity(Granularity.HOUR.toString());
        j.setEffectiveQueryTime(123456);
        int runtime = 1234;
        List<TimeSeries> tslist = Collections.emptyList();
        DetectorService ds = mock(DetectorService.class);
        JobExecutionService jes = mock(JobExecutionService.class);
        DetectionTask et = new DetectionTask(j, runtime, tslist, ds, jes);
        List<AnomalyReport> arlist = Lists.newArrayList(new AnomalyReport(), new AnomalyReport(), new AnomalyReport());
        when(ds.runDetection(any(), anyDouble(), any(), anyInt(), anyString(), any(), anyInt())).thenReturn(new ArrayList<>());
        when(jes.getReports(any(), any())).thenReturn(arlist);
        when(jes.getSingletonReport(any())).thenReturn(new AnomalyReport());
        when(jes.getAnomalyReportAccessor()).thenReturn(ara);
        doNothing().when(ara).deleteAnomalyReportsForJobAtTime(anyString(), anyString(), anyString());
        et.run();
        assertEquals(et.getReports().size(), 3);
    }

}
