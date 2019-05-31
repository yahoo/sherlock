/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.google.gson.JsonArray;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.service.TimeSeriesParserService;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DBTestHelper;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.MetricMeta;
import com.yahoo.egads.data.TimeSeries;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 * Test for JobExecutionService.
 */
@SuppressWarnings("FieldCanBeLocal")
public class JobExecutionServiceTest {

    private JobExecutionService jes;

    private static JobExecutionService getMock() {
        return mock(JobExecutionService.class);
    }

    private static void inject(JobExecutionService jes, String n, Object v) {
        try {
            Field f = JobExecutionService.class.getDeclaredField(n);
            f.setAccessible(true);
            f.set(jes, v);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail(e.toString());
        }
    }

    private DruidClusterAccessor dca;
    private JobMetadataAccessor jma;
    private AnomalyReportAccessor ara;
    private DetectorService ds;
    private SchedulerService ss;
    private EmailService es;
    private EmailMetadataAccessor ema;
    private TimeSeriesParserService ps;

    private void initMocks() {
        jes = getMock();
        ServiceFactory sf = mock(ServiceFactory.class);
        dca = mock(DruidClusterAccessor.class);
        jma = mock(JobMetadataAccessor.class);
        ds = mock(DetectorService.class);
        ss = mock(SchedulerService.class);
        es = mock(EmailService.class);
        ara = mock(AnomalyReportAccessor.class);
        ps = mock(TimeSeriesParserService.class);
        ema = mock(EmailMetadataAccessor.class);
        inject(jes, sf);
        inject(jes, dca);
        inject(jes, jma);
        inject(jes, "anomalyReportAccessor", ara);
        inject(jes, "emailMetadataAccessor", ema);
        when(sf.newEmailServiceInstance()).thenReturn(es);
        when(sf.newSchedulerServiceInstance()).thenReturn(ss);
        when(sf.newDetectorServiceInstance()).thenReturn(ds);
        when(sf.newTimeSeriesParserServiceInstance()).thenReturn(ps);
    }

    private static void inject(JobExecutionService jes, ServiceFactory sf) {
        inject(jes, "serviceFactory", sf);
    }

    private static void inject(JobExecutionService jes, DruidClusterAccessor dca) {
        inject(jes, "druidClusterAccessor", dca);
    }

    private static void inject(JobExecutionService jes, JobMetadataAccessor jma) {
        inject(jes, "jobMetadataAccessor", jma);
    }

    @Test
    public void testExecute() throws SherlockException, IOException {
        initMocks();
        doCallRealMethod().when(jes).execute(any(JobMetadata.class));
        AnomalyReport anomalyReport = DBTestHelper.getNewReport();
        anomalyReport.setStatus(Constants.WARNING);
        when(jes.getReports(any(), any())).thenReturn(Collections.singletonList(anomalyReport));
        when(ema.checkEmailsInInstantIndex(anyList())).thenReturn(new ArrayList());
        jes.execute(DBTestHelper.getNewJob());
        verify(jes, times(1)).execute(any(JobMetadata.class));
        verify(es, times(0)).processEmailReports(any(JobMetadata.class), anyList(), anyListOf(AnomalyReport.class));
        verify(ara, times(1)).putAnomalyReports(anyListOf(AnomalyReport.class), anyList());
        // check for instant email found in the index
        when(ema.checkEmailsInInstantIndex(anyList())).thenReturn(Arrays.asList("email"));
        jes.execute(DBTestHelper.getNewJob());
        verify(jes, times(2)).execute(any(JobMetadata.class));
        verify(es, times(1)).processEmailReports(any(JobMetadata.class), anyList(), anyListOf(AnomalyReport.class));
        verify(ara, times(2)).putAnomalyReports(anyListOf(AnomalyReport.class), anyList());
        // check for error report case
        anomalyReport.setStatus(Constants.ERROR);
        jes.execute(DBTestHelper.getNewJob());
        verify(jes, times(3)).execute(any(JobMetadata.class));
        verify(es, times(2)).processEmailReports(any(JobMetadata.class), anyList(), anyListOf(AnomalyReport.class));
        verify(ara, times(3)).putAnomalyReports(anyListOf(AnomalyReport.class), anyList());
    }

    @Test
    public void testExecuteSingletonJob() throws SherlockException {
        initMocks();
        when(jes.executeJob(any(), any())).thenThrow(new SherlockException());
        doCallRealMethod().when(jes).execute(any(JobMetadata.class));
        when(jes.getReports(any(), any())).thenReturn(new ArrayList<>());
        AnomalyReport a = new AnomalyReport();
        a.setStatus(Constants.ERROR);
        when(jes.getSingletonReport(any())).thenReturn(a);
        JobMetadata job = DBTestHelper.getNewJob();
        job.setGranularity(Granularity.HOUR.toString());
        job.setEffectiveQueryTime(123456);
        jes.execute(job);
        verify(jes, times(1)).execute(any(JobMetadata.class));
        verify(jes, times(1)).unscheduleErroredJob(any());
    }

    @Test
    public void testUnscheduleJob() throws IOException {
        initMocks();
        JobMetadata job = new JobMetadata();
        job.setJobId(1);
        doCallRealMethod().when(jes).unscheduleErroredJob(any());
        jes.unscheduleErroredJob(job);
        verify(jma, times(1)).putJobMetadata(job);
    }

    @Test
    public void testGetReports() {
        Anomaly an = new Anomaly();
        an.metricMetaData = new MetricMeta();
        an.metricMetaData.id = "4";
        an.metricMetaData.name = "name";
        an.metricMetaData.source = "sources";
        an.intervals = new Anomaly.IntervalSequence();
        an.addInterval(1, 11, 1.11f);
        an.intervals.get(0).actualVal = 12.0f;
        an.intervals.get(0).expectedVal = 7.0f;
        JobMetadata job = new JobMetadata();
        job.setUrl("http://url.com");
        job.setJobId(1);
        job.setGranularity(Granularity.HOUR.toString());
        job.setEffectiveQueryTime(123456);
        job.setFrequency("day");
        job.setJobStatus(JobStatus.RUNNING.getValue());
        List<Anomaly> anomalies = new ArrayList<>(Arrays.asList(an));
        initMocks();
        when(jes.getReports(any(), any())).thenCallRealMethod();
        List<AnomalyReport> reports = jes.getReports(new ArrayList<>(), job);
        Assert.assertEquals(reports.size(), 0);
        reports = jes.getReports(anomalies, job);
        assertEquals(reports.size(), 1);
        AnomalyReport rep = reports.get(0);
        assertEquals(rep.getJobId(), (Integer) 1);
        // test nodata and warning at the same time for single job
        Anomaly an1 = new Anomaly();
        an1.metricMetaData = new MetricMeta();
        an1.metricMetaData.id = "4";
        an1.metricMetaData.name = JobStatus.NODATA.getValue();
        an1.metricMetaData.source = "source1";
        an1.intervals = new Anomaly.IntervalSequence();
        anomalies.add(an1);
        List<AnomalyReport> allReports = jes.getReports(anomalies, job);
        Assert.assertEquals(allReports.size(), 2);
        Assert.assertEquals(job.getJobStatus(), JobStatus.RUNNING.getValue());
        anomalies.remove(0);
        Assert.assertEquals(anomalies.size(), 1);
        Assert.assertEquals(anomalies.get(0).metricMetaData.name, JobStatus.NODATA.getValue());
        allReports = jes.getReports(anomalies, job);
        Assert.assertEquals(allReports.size(), 0);
        Assert.assertEquals(job.getJobStatus(), JobStatus.NODATA.getValue());
    }

    @Test
    public void testGetSingletonReport() {
        JobMetadata job = new JobMetadata();
        job.setJobId(1);
        job.setUrl("http://url.com");
        job.setJobStatus(JobStatus.ERROR.getValue());
        job.setGranularity(Granularity.HOUR.toString());
        job.setEffectiveQueryTime(123456);
        Anomaly an = new Anomaly();
        an.metricMetaData = new MetricMeta();
        an.metricMetaData.id = "id";
        an.metricMetaData.name = "name";
        an.metricMetaData.source = "source";
        initMocks();
        when(jes.getSingletonReport(any())).thenCallRealMethod();
        AnomalyReport result = jes.getSingletonReport(job);
        assertEquals(result.getJobId(), (Integer) 1);
        assertEquals(result.getStatus(), Constants.ERROR);
        assertNotNull(result.getUniqueId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStartJobSuccess() throws Exception {
        initMocks();
        DruidCluster c = mock(DruidCluster.class);
        doCallRealMethod().when(jes).executeJob(any(), any());
        List ml = mock(List.class);
        when(ds.detect(any(), any())).thenReturn(ml);
        JobMetadata jm = mock(JobMetadata.class);
        when(jm.getClusterId()).thenReturn(5);
        jes.executeJob(jm, c);
    }

    private static Anomaly mockAnomaly() {
        Anomaly a = mock(Anomaly.class);
        a.metricMetaData = mock(MetricMeta.class);
        a.intervals = new Anomaly.IntervalSequence();
        Anomaly.Interval i = new Anomaly.Interval();
        i.startTime = 100;
        i.endTime = 1000L;
        a.intervals.add(i);
        a.id = "4";
        return a;
    }

    @Test
    public void testPerformBackfillJob() throws Exception {
        initMocks();
        Query query = mock(Query.class);
        when(query.getGranularityRange()).thenReturn(1);
        JsonArray response = new JsonArray();
        when(ds.queryDruid(any(), any())).thenReturn(response);
        @SuppressWarnings("unchecked")
        List<TimeSeries>[] fillSeriesList = (List<TimeSeries>[]) new List[3];
        when(ps.subseries(any(), anyLong(), anyLong(), any(), anyInt(), anyInt())).thenReturn(fillSeriesList);
        doCallRealMethod().when(jes).performBackfillJob(any(), any(), any(), anyInt(), anyInt(), any(), anyInt());
        EgadsTask ftask = mock(EgadsTask.class);
        when(ftask.getReports()).thenReturn(Collections.singletonList(new AnomalyReport()));
        when(jes.createTask(any(), anyInt(), any(), any())).thenReturn(ftask);
        JobMetadata j = DBTestHelper.getNewJob();
        DruidCluster c = new DruidCluster();
        jes.performBackfillJob(j, c, query, 123, 128, Granularity.HOUR, 10);
        verify(ara, times(1)).putAnomalyReports(any(), anyList());
    }

    @Test
    public void testCreateEgadsTask() {
        initMocks();
        when(jes.createTask(any(), anyInt(), any(), any())).thenCallRealMethod();
        EgadsTask et = jes.createTask(new JobMetadata(), 123, null, ds);
        assertNull(et.getReports());
    }

    @Test
    public void testExecuteJob() throws SherlockException, DruidException {
        initMocks();
        when(ds.detect(any(), any())).thenReturn(Collections.emptyList());
        when(jes.executeJob(any(), any())).thenCallRealMethod();
        List<Anomaly> res = jes.executeJob(new JobMetadata(), new DruidCluster());
        assertEquals(res.size(), 0);
        when(ds.detect(any(), any())).thenThrow(new SherlockException());
        try {
            jes.executeJob(new JobMetadata(), new DruidCluster());
        } catch (SherlockException e) {
            return;
        }
        fail();
    }

    @Test
    public void testExecuteJobConfigs() throws Exception {
        initMocks();
        when(ds.detect(any(), anyDouble(), any(), any(EgadsConfig.class), anyString(), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(jes.executeJob(any(), any(), any(), any())).thenCallRealMethod();
        assertEquals(1, jes.executeJob(new JobMetadata(),
                new DruidCluster(), mock(Query.class), mock(EgadsConfig.class)).size());
        when(ds.detect(any(), anyDouble(), any(), any(EgadsConfig.class), anyString(), anyInt()))
                .thenThrow(new SherlockException());
        try {
            jes.executeJob(new JobMetadata(),
                    new DruidCluster(), mock(Query.class), mock(EgadsConfig.class));
        } catch (SherlockException e) {
            return;
        }
        fail();
    }

}
