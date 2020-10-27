/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock;

import com.beust.jcommander.internal.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.exception.EmailNotFoundException;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.model.JobTimeline;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.service.JobExecutionService;
import com.yahoo.sherlock.service.SchedulerService;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.DruidQueryService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.settings.CLISettingsTest;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.QueryConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DBTestHelper;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JsonDumper;
import org.testng.Assert;
import org.testng.annotations.Test;

import spark.HaltException;
import spark.ModelAndView;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class RoutesTest {

    private Request req;
    private Response res;
    private DruidQueryService qs;
    private DetectorService ds;
    private DruidClusterAccessor dca;
    private JobMetadataAccessor jma;
    private AnomalyReportAccessor ara;
    private EmailMetadataAccessor ema;
    private JobExecutionService jes;
    private ThymeleafTemplateEngine tte;
    private ServiceFactory sf;
    private SchedulerService ss;

    private void mocks() {
        req = mock(Request.class);
        res = mock(Response.class);
        qs = mock(DruidQueryService.class);
        ds = mock(DetectorService.class);
        jes = mock(JobExecutionService.class);
        sf = mock(ServiceFactory.class);
        ss = mock(SchedulerService.class);
        when(sf.newJobExecutionService()).thenReturn(jes);
        when(sf.newDetectorServiceInstance()).thenReturn(ds);
        when(sf.newDruidQueryServiceInstance()).thenReturn(qs);
        inject("serviceFactory", sf);
        tte = mock(ThymeleafTemplateEngine.class);
        inject("thymeleaf", tte);
        dca = mock(DruidClusterAccessor.class);
        inject("clusterAccessor", dca);
        jma = mock(JobMetadataAccessor.class);
        inject("jobAccessor", jma);
        ara = mock(AnomalyReportAccessor.class);
        inject("reportAccessor", ara);
        ema = mock(EmailMetadataAccessor.class);
        inject("emailMetadataAccessor", ema);
        @SuppressWarnings("unchecked") Map<String, Object> dp = (Map<String, Object>) mock(Map.class);
        inject("defaultParams", dp);
    }

    private static void inject(String name, Object mock) {
        CLISettingsTest.setField(CLISettingsTest.getField(name, Routes.class), mock);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> params(Object o) {
        return (Map<String, Object>) o;
    }

    private static Map<String, Object> params(ModelAndView mav) {
        return params(mav.getModel());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInitParams() {
        Field defaultParamsField = CLISettingsTest.getField("defaultParams", Routes.class);
        Object prevValue = CLISettingsTest.fieldVal(defaultParamsField);
        try {
            CLISettingsTest.setField(defaultParamsField, null);
            Routes.initParams();
            Object curValue = CLISettingsTest.fieldVal(defaultParamsField);
            assertNotNull(curValue);
            Map<String, Object> params = (Map<String, Object>) curValue;
            assertTrue(params.containsKey(Constants.PROJECT));
            assertTrue(params.containsKey(Constants.TITLE));
            assertTrue(params.containsKey(Constants.VERSION));
        } finally {
            CLISettingsTest.setField(defaultParamsField, prevValue);
        }
    }

    @Test
    public void testInitAccessors() {
        CLISettingsTest.setField(CLISettingsTest.getField("reportAccessor", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("deletedJobAccessor", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("clusterAccessor", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("jobAccessor", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("emailMetadataAccessor", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("jsonDumper", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("serviceFactory", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("schedulerService", Routes.class), null);
        try {
            Routes.initServices();
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("reportAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("deletedJobAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("clusterAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("emailMetadataAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("jobAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("jsonDumper", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("serviceFactory", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("schedulerService", Routes.class)));
        } finally {
            CLISettingsTest.setField(CLISettingsTest.getField("reportAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("deletedJobAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("clusterAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("jobAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("emailMetadataAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("jsonDumper", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("serviceFactory", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("schedulerService", Routes.class), null);
        }
    }

    @Test
    public void testViewHomePage() {
        Routes.initParams();
        mocks();
        ModelAndView mav = Routes.viewHomePage(req, res);
        assertEquals(mav.getViewName(), "homePage");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewInstantAnomalyFormEmptyDruidList() throws IOException {
        Routes.initParams();
        mocks();
        when(dca.getDruidClusterList()).thenThrow(new IOException());
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewInstantAnomalyJobForm(req, res);
        assertEquals(params(mav.getModel()).get(Constants.INSTANTVIEW), "true");
        List<DruidCluster> clusters = (List<DruidCluster>) params(mav.getModel()).get(Constants.DRUID_CLUSTERS);
        assertEquals(clusters.size(), 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewInstantAnomalyForm() throws IOException {
        Routes.initParams();
        mocks();
        DruidCluster cl = new DruidCluster();
        cl.setClusterId(5);
        List<DruidCluster> cls = Collections.singletonList(cl);
        when(dca.getDruidClusterList()).thenReturn(cls);
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewInstantAnomalyJobForm(req, res);
        List<DruidCluster> clusters = (List<DruidCluster>) params(mav.getModel()).get(Constants.DRUID_CLUSTERS);
        assertEquals(clusters.size(), 1);
        assertEquals(clusters.get(0).getClusterId(), (Integer) 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewNewAnomalyForm() throws IOException {
        Routes.initParams();
        mocks();
        DruidCluster cl = new DruidCluster();
        cl.setClusterId(5);
        List<DruidCluster> cls = Collections.singletonList(cl);
        when(dca.getDruidClusterList()).thenReturn(cls);
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewNewAnomalyJobForm(req, res);
        List<DruidCluster> clusters = (List<DruidCluster>) params(mav.getModel()).get(Constants.DRUID_CLUSTERS);
        assertEquals(clusters.size(), 1);
        assertEquals(clusters.get(0).getClusterId(), (Integer) 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewNewAnomalyFormEmptyDruidList() throws IOException {
        Routes.initParams();
        mocks();
        when(dca.getDruidClusterList()).thenThrow(new IOException());
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewNewAnomalyJobForm(req, res);
        assertTrue(params(mav.getModel()).containsKey(Constants.ERROR));
    }

    @Test
    public void testSaveUserJob() throws Exception {
        mocks();
        when(req.body()).thenReturn(
                "{" +
                        "\"clusterId\":\"1\"," +
                        "\"ownerEmail\":\"someone@something.com\"" +
                        "}"
        );
        when(sf.newEmailServiceInstance()).thenCallRealMethod();
        DruidQueryService dqs = mock(DruidQueryService.class);
        Query query = mock(Query.class);
        JsonObject jo = new JsonObject();
        when(query.getQueryJsonObject()).thenReturn(jo);
        when(dqs.build(anyString(), any(Granularity.class), anyInt(), anyInt(), anyInt())).thenReturn(query);
        when(sf.newDruidQueryServiceInstance()).thenReturn(dqs);
        doAnswer(iom -> {
                Object[] args = iom.getArguments();
                ((JobMetadata) args[0]).setJobId(10);
                assertEquals(((JobMetadata) args[0]).getClusterId(), (Integer) 1);
                return null;
            }
        ).when(jma).putJobMetadata(any(JobMetadata.class));
        inject("clusterAccessor", dca);
        inject("jobAccessor", jma);
        inject("serviceFactory", sf);
        assertEquals(Routes.saveUserJob(req, res), "10");
        verify(res, times(1)).status(200);
    }

    @Test
    public void testSaveUserJobException() throws Exception {
        mocks();
        when(req.body()).thenReturn(
                "{" +
                        "\"clusterId\":\"1\"," +
                        "\"ownerEmail\":\"someone@something.com\"" +
                        "}"
        );
        when(sf.newEmailServiceInstance()).thenCallRealMethod();
        DruidQueryService dqs = mock(DruidQueryService.class);
        when(dqs.build(anyString(), any(Granularity.class), anyInt(), anyInt(), anyInt())).thenThrow(new SherlockException("exception"));
        when(sf.newDruidQueryServiceInstance()).thenReturn(dqs);
        inject("serviceFactory", sf);
        assertEquals(Routes.saveUserJob(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testDeleteJob() throws SchedulerException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        inject("jobAccessor", jma);
        inject("schedulerService", ss);
        assertEquals(Routes.deleteJob(req, res), Constants.SUCCESS);
        verify(ss, times(1)).stopJob(1);
    }

    @Test
    public void testDeleteJobException() throws IOException, JobNotFoundException {
        mocks();
        when(req.params(anyString())).thenReturn("1");
        inject("schedulerService", ss);
        doThrow(new IOException("fake")).when(jma).deleteJobMetadata(anyInt());
        inject("jobAccessor", jma);
        assertEquals(Routes.deleteJob(req, res), "fake");
        verify(res, times(1)).status(500);
        when(req.params(anyString())).thenReturn("%3c%73%63%72%69%70%74%3e%61%6c%65%72%74%28%31%29%3c%2f%73%63%72%69%70%74%3e");
        assertEquals(Routes.deleteJob(req, res), "Invalid Job!");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewJobsList() throws IOException {
        Routes.initParams();
        mocks();
        List<JobMetadata> jobsList = (List<JobMetadata>) mock(List.class);
        when(jobsList.size()).thenReturn(1);
        when(jma.getJobMetadataList()).thenReturn(jobsList);
        inject("jobAccessor", jma);
        JobTimeline jobTimeline = mock(JobTimeline.class);
        inject("jobTimeline", jobTimeline);
        when(jobTimeline.getCurrentTimeline(any())).thenReturn(null);
        ModelAndView mav = Routes.viewJobsList(req, res);
        assertTrue(params(mav).containsKey(Constants.TITLE));
        assertEquals(((List) params(mav).get("jobs")).size(), 1);
        assertEquals(mav.getViewName(), "listJobs");
    }

    @Test
    public void testViewJobsListException() throws IOException {
        Routes.initParams();
        mocks();
        when(jma.getJobMetadataList()).thenThrow(new IOException("exception"));
        inject("jobAccessor", jma);
        ModelAndView mav = Routes.viewJobsList(req, res);
        assertTrue(params(mav).containsKey(Constants.ERROR));
        assertEquals(params(mav).get(Constants.ERROR), "exception");
        assertEquals(mav.getViewName(), "listJobs");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewDeletedJobsList() throws IOException {
        Routes.initParams();
        mocks();
        List<JobMetadata> ljm = (List<JobMetadata>) mock(List.class);
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        inject("deletedJobAccessor", dma);
        when(dma.getDeletedJobMetadataList()).thenReturn(ljm);
        ModelAndView mav = Routes.viewDeletedJobsList(req, res);
        assertTrue(params(mav).containsKey(Constants.TITLE));
        assertEquals(params(mav).get(Constants.DELETEDJOBSVIEW), "true");
        assertEquals(mav.getViewName(), "listJobs");
        assertEquals(params(mav).get("jobs"), ljm);
    }

    @Test
    public void testDeletedJobsListException() throws IOException {
        Routes.initParams();
        mocks();
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        inject("deletedJobAccessor", dma);
        when(dma.getDeletedJobMetadataList()).thenThrow(new IOException("exception"));
        ModelAndView mav = Routes.viewDeletedJobsList(req, res);
        assertTrue(params(mav).containsKey(Constants.ERROR));
    }

    @Test
    public void testViewJobInfo() throws IOException, JobNotFoundException, ClusterNotFoundException {
        Routes.initParams();
        mocks();
        inject("jobAccessor", jma);
        inject("clusterAccessor", dca);
        when(req.params(Constants.ID)).thenReturn("1");
        JobMetadata jm = mock(JobMetadata.class);
        DruidCluster dc = DBTestHelper.getNewDruidCluster();
        dc.setClusterId(2);
        when(jma.getJobMetadata("1")).thenReturn(jm);
        when(jm.getClusterId()).thenReturn(2);
        when(dca.getDruidClusterList()).thenReturn(Collections.singletonList(dc));
        ModelAndView mav = Routes.viewJobInfo(req, res);
        assertTrue(params(mav).containsKey(Constants.TITLE));
        assertEquals(params(mav).get("job"), jm);
        assertEquals(params(mav).get(Constants.DRUID_CLUSTERS), Collections.singletonList(dc));
    }

    @Test(expectedExceptions = HaltException.class)
    public void testViewJobInfoException() throws IOException, JobNotFoundException {
        Routes.initParams();
        mocks();
        inject("jobAccessor", jma);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("fake"));
        inject("thymeleaf", tte);
        when(tte.render(anyObject())).thenReturn("404");
        Routes.viewJobInfo(req, res);
    }

    @Test
    public void testViewDeletedJobInfo() throws IOException, JobNotFoundException, ClusterNotFoundException {
        Routes.initParams();
        mocks();
        DruidCluster dc = DBTestHelper.getNewDruidCluster();
        when(dca.getDruidClusterList()).thenReturn(Collections.singletonList(dc));
        inject("clusterAccessor", dca);
        JobMetadata jm = mock(JobMetadata.class);
        when(req.params(Constants.ID)).thenReturn("1");
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        when(dma.getDeletedJobMetadata("1")).thenReturn(jm);
        inject("deletedJobAccessor", dma);
        ModelAndView mav = Routes.viewDeletedJobInfo(req, res);
        assertEquals(params(mav).get(Constants.DELETEDJOBSVIEW), "true");
        assertEquals(params(mav).get("job"), jm);
        assertEquals(params(mav).get(Constants.DRUID_CLUSTERS), Collections.singletonList(dc));
    }

    @Test(expectedExceptions = HaltException.class)
    public void testViewDeletedJobInfoException() throws IOException, JobNotFoundException {
        Routes.initParams();
        mocks();
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        when(dma.getDeletedJobMetadata(anyString())).thenThrow(new IOException("fake"));
        inject("deletedJobAccessor", dma);
        inject("thymeleaf", tte);
        when(tte.render(anyObject())).thenReturn("404");
        Routes.viewDeletedJobInfo(req, res);
    }

    @Test
    public void testUpdateJobInfo() throws Exception {
        String body = "{\"clusterId\":\"1\",\"hoursOfLag\":\"10\",\"granularity\":\"day\",\"frequency\":\"day\",\"sigmaThreshold\":\"3\",\"ownerEmail\":\"someone@something.com\",\"query\":\"{}\"}";
        mocks();
        when(req.body()).thenReturn(body);
        when(req.params(Constants.ID)).thenReturn("1");
        JobMetadata jm = new JobMetadata();
        jm.setJobId(1);
        jm.setGranularity("day");
        jm.setFrequency("day");
        jm.setSigmaThreshold(3.0);
        jm.setJobStatus("RUNNING");
        jm.setUserQuery("query");
        jm.setQuery("query");
        jm.setHoursOfLag(10);
        jm.setClusterId(1);
        when(jma.getJobMetadata("1")).thenReturn(jm);
        DruidQueryService dqs = mock(DruidQueryService.class);
        Query q = mock(Query.class);
        JsonObject j = new JsonObject();
        when(sf.newEmailServiceInstance()).thenCallRealMethod();
        when(q.getQueryJsonObject()).thenReturn(j);
        when(dqs.build(anyString(), any(Granularity.class), anyInt(), anyInt(), anyInt())).thenReturn(q);
        when(sf.newDruidQueryServiceInstance()).thenReturn(dqs);
        inject("serviceFactory", sf);
        inject("jobAccessor", jma);
        inject("schedulerService", ss);
        assertEquals(Routes.updateJobInfo(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(jma, times(1)).putJobMetadata(jm);
    }

    @Test
    public void testUpdateJobInfoException() throws IOException, JobNotFoundException {
        mocks();
        when(req.params(anyString())).thenReturn("1");
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("exception"));
        when(req.body()).thenReturn("{\"ownerEmail\":\"someone@something.com\"}");
        inject("jobAccessor", jma);
        inject("serviceFactory", sf);
        assertEquals(Routes.updateJobInfo(req, res), "exception");
        verify(res, times(1)).status(500);
        when(req.params(anyString())).thenReturn("1%24%");
        assertEquals(Routes.updateJobInfo(req, res), "Invalid Job!");
    }

    @Test
    public void testLaunchJob() throws IOException, JobNotFoundException, SchedulerException, ClusterNotFoundException {
        mocks();
        JobMetadata jm = new JobMetadata();
        inject("jobAccessor", jma);
        inject("schedulerService", ss);
        DruidCluster cluster = new DruidCluster();
        cluster.setHoursOfLag(10);
        jm.setClusterId(123);
        jm.setJobId(12);
        when(dca.getDruidCluster(123)).thenReturn(cluster);
        when(req.params(Constants.ID)).thenReturn("1");
        when(jma.getJobMetadata("1")).thenReturn(jm);
        assertEquals(Routes.launchJob(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(jma, times(1)).putJobMetadata(jm);
        verify(ss, times(1)).scheduleJob(jm);
        assertEquals(jm.getJobStatus(), "RUNNING");
    }

    @Test
    public void testLaunchJobException() throws IOException, JobNotFoundException {
        mocks();
        when(req.params(anyString())).thenReturn("1");
        inject("jobAccessor", jma);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("exception"));
        assertEquals(Routes.launchJob(req, res), "exception");
        verify(res, times(1)).status(500);
        when(req.params(anyString())).thenReturn("%3c%73%63%72%69%70%74%3e%61%6c%65%72%74%28%31%29%3c%2f%73%63%72%69%70%74%3e");
        assertEquals(Routes.deleteJob(req, res), "Invalid Job!");
    }

    @Test
    public void testStopJob() throws IOException, JobNotFoundException, SchedulerException {
        mocks();
        JobMetadata jm = new JobMetadata();
        when(req.params(Constants.ID)).thenReturn("1");
        inject("jobAccessor", jma);
        jm.setJobId(1);
        jm.setFrequency("day");
        inject("schedulerService", ss);
        when(jma.getJobMetadata("1")).thenReturn(jm);
        assertEquals(Routes.stopJob(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(jma, times(1)).putJobMetadata(jm);
        verify(ss, times(1)).stopJob(1);
    }

    @Test
    public void testStopJobException() throws IOException, JobNotFoundException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("exception"));
        inject("jobAccessor", jma);
        assertEquals(Routes.stopJob(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testViewJobReport() throws Exception {
        Routes.initParams();
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        when(req.params(Constants.FREQUENCY_PARAM)).thenReturn("day");
        JobMetadata jm = new JobMetadata();
        List<AnomalyReport> lar = Collections.emptyList();
        when(ara.getAnomalyReportsForJob(anyString(), anyString())).thenReturn(lar);
        inject("reportAccessor", ara);
        inject("jobAccessor", jma);
        when(jma.getJobMetadata(anyString())).thenReturn(jm);
        ModelAndView mav = Routes.viewJobReport(req, res);
        assertEquals(mav.getViewName(), "report");
        assertEquals(params(mav).get(Constants.FREQUENCY), "day");
        assertTrue(params(mav).containsKey(Constants.TIMELINE_POINTS));
    }

    @Test
    public void testSendJobReport() throws IOException {
        Routes.initParams();
        mocks();
        when(tte.render(any(ModelAndView.class))).thenReturn("table");
        AnomalyReport a = mock(AnomalyReport.class);
        List<AnomalyReport> lar = Collections.singletonList(a);
        when(ara.getAnomalyReportsForJobAtTime(anyString(), anyString(), anyString())).thenReturn(lar);
        when(req.body()).thenReturn("{\"" + Constants.SELECTED_DATE + "\":\"5678\", \"frequency\":\"day\"}");
        when(req.params(anyString())).thenReturn("4");
        inject("thymeleaf", tte);
        inject("reportAccessor", ara);
        assertEquals(Routes.sendJobReport(req, res), "table");
        verify(res, times(1)).status(200);
        when(req.body()).thenReturn("{\"" + Constants.SELECTED_DATE + "\":\"-5678\", \"frequency\":\"dayss\"}");
        assertEquals(Routes.sendJobReport(req, res), "Invalid Request");
    }

    @Test
    public void testSendJobReportException() throws IOException {
        Routes.initParams();
        mocks();
        when(ara.getAnomalyReportsForJobAtTime(anyString(), anyString(), anyString())).thenThrow(new IOException("exception"));
        inject("reportAccessor", ara);
        when(req.body()).thenReturn("{\"" + Constants.SELECTED_DATE + "\":\"5678\", \"frequency\":\"day\"}");
        when(req.params(anyString())).thenReturn("2");
        assertTrue(Routes.sendJobReport(req, res).contains("exception"));
        verify(res, times(1)).status(500);
    }

    @Test
    public void testViewNewDruidClusterForm() {
        Routes.initParams();
        ModelAndView mav = Routes.viewNewDruidClusterForm(req, res);
        assertEquals(mav.getViewName(), "druidForm");
    }

    @Test
    public void testAddNewDruidCluster() throws IOException {
        mocks();
        String body =
                "{" +
                        "\"clusterName\":\"name\"," +
                        "\"brokerHost\":\"localhost\"," +
                        "\"brokerPort\":\"5080\"," +
                        "\"brokerEndpoint\":\"druid/v2\"" +
                        "}";
        when(req.body()).thenReturn(body);
        inject("clusterAccessor", dca);
        doAnswer(iom -> {
                ((DruidCluster) iom.getArguments()[0]).setClusterId(1);
                return null;
            }
        ).when(dca).putDruidCluster(any(DruidCluster.class));
        assertEquals(Routes.addNewDruidCluster(req, res), "1");
        verify(res, times(1)).status(200);
    }

    @Test
    public void testAddNewClusterInvalidCluster() {
        mocks();
        String body = "{}";
        when(req.body()).thenReturn(body);
        assertEquals(Routes.addNewDruidCluster(req, res), "Cluster name cannot be empty");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testAddNewClusterException() throws IOException {
        mocks();
        String body =
                "{" +
                        "\"clusterName\":\"name\"," +
                        "\"brokerHost\":\"localhost\"," +
                        "\"brokerPort\":\"5080\"," +
                        "\"brokerEndpoint\":\"druid/v2\"" +
                        "}";
        when(req.body()).thenReturn(body);
        inject("clusterAccessor", dca);
        doThrow(new IOException("exception")).when(dca).putDruidCluster(any(DruidCluster.class));
        assertEquals(Routes.addNewDruidCluster(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testViewDruidClusterList() throws IOException {
        mocks();
        Routes.initParams();
        inject("clusterAccessor", dca);
        when(dca.getDruidClusterList()).thenReturn(Collections.emptyList());
        ModelAndView mav = Routes.viewDruidClusterList(req, res);
        assertEquals(mav.getViewName(), "druidList");
        assertEquals(((List) params(mav).get("clusters")).size(), 0);
    }

    @Test
    public void testViewDruidClusterListException() throws IOException {
        mocks();
        Routes.initParams();
        inject("clusterAccessor", dca);
        when(dca.getDruidClusterList()).thenThrow(new IOException("exception"));
        ModelAndView mav = Routes.viewDruidClusterList(req, res);
        assertEquals(params(mav).get(Constants.ERROR), "exception");
    }

    @Test
    public void testViewDruidCluster() throws IOException, ClusterNotFoundException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        DruidCluster dc = new DruidCluster();
        dc.setClusterId(1);
        when(dca.getDruidCluster("1")).thenReturn(dc);
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewDruidCluster(req, res);
        assertEquals(mav.getViewName(), "druidInfo");
        assertEquals(((DruidCluster) params(mav).get("cluster")).getClusterId(), (Integer) 1);
    }

    @Test(expectedExceptions = HaltException.class)
    public void testViewDruidClusterException() throws IOException, ClusterNotFoundException {
        mocks();
        when(dca.getDruidCluster(anyString())).thenThrow(new IOException("exception"));
        inject("clusterAccessor", dca);
        inject("thymeleaf", tte);
        when(tte.render(anyObject())).thenReturn("404");
        Routes.viewDruidCluster(req, res);
    }

    @Test
    public void testDeleteDruidCluster() throws IOException, ClusterNotFoundException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(Collections.emptyList());
        inject("jobAccessor", jma);
        inject("clusterAccessor", dca);
        assertEquals(Routes.deleteDruidCluster(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(dca, times(1)).deleteDruidCluster("1");
    }

    @Test
    public void testDeleteDruidClusterAssociatedJobs() throws IOException {
        mocks();
        inject("jobAccessor", jma);
        JobMetadata jm = new JobMetadata();
        jm.setJobId(2);
        List<JobMetadata> jlist = Collections.singletonList(jm);
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(jlist);
        when(req.params(anyString())).thenReturn("1");
        assertEquals(
                Routes.deleteDruidCluster(req, res),
                "Cannot delete cluster with 1 associated jobs"
        );
        verify(res, times(1)).status(400);
        when(req.params(anyString())).thenReturn("1cv%32c%32");
        assertEquals(
            Routes.deleteDruidCluster(req, res),
            "Invalid Cluster!"
        );
    }

    @Test
    public void testDeleteDruidClusterException() throws IOException, ClusterNotFoundException {
        mocks();
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(Collections.emptyList());
        when(req.params(anyString())).thenReturn("100");
        doThrow(new IOException("exception")).when(dca).deleteDruidCluster(anyString());
        inject("jobAccessor", jma);
        inject("clusterAccessor", dca);
        assertEquals(Routes.deleteDruidCluster(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testUpdateDruidCluster() throws IOException, ClusterNotFoundException, SchedulerException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        inject("jobAccessor", jma);
        JsonObject dcjson = new JsonObject();
        dcjson.addProperty("clusterName", "name");
        dcjson.addProperty("clusterDescription", "descr");
        dcjson.addProperty("brokerHost", "hostname");
        dcjson.addProperty("brokerPort", "431");
        dcjson.addProperty("brokerEndpoint", "druid/v2");
        dcjson.addProperty("hoursOfLag", "12");
        String body = new Gson().toJson(dcjson);
        when(req.body()).thenReturn(body);
        JobMetadata j = DBTestHelper.getNewJob();
        j.setHoursOfLag(10);
        j.setJobStatus("RUNNING");
        inject("schedulerService", ss);
        doNothing().when(ss).stopAndReschedule(anyList());
        doNothing().when(jma).putJobMetadata(anyList());
        DruidCluster dc = new DruidCluster();
        dc.setClusterId(1);
        dc.setBrokerHost("localhost");
        dc.setHoursOfLag(10);
        dc.setBrokerEndpoint("druid/v2");
        dc.setBrokerPort(1234);
        dc.setClusterName("name");
        when(dca.getDruidCluster(anyString())).thenReturn(dc);
        List<JobMetadata> jlist = new ArrayList<>();
        jlist.add(j);
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(jlist);
        inject("clusterAccessor", dca);
        assertEquals(Routes.updateDruidCluster(req, res), Constants.SUCCESS);
        assertEquals(dc.getBrokerHost(), "hostname");
        assertEquals(dc.getBrokerPort(), (Integer) 431);
        assertEquals(j.getHoursOfLag(), (Integer) 12);
        verify(res, times(1)).status(200);
        verify(dca, times(1)).putDruidCluster(dc);
    }

    @Test
    public void testUpdateDruidClusterException() throws IOException, ClusterNotFoundException {
        mocks();
        inject("clusterAccessor", dca);
        when(req.params(Constants.ID)).thenReturn("1");
        when(dca.getDruidCluster(anyString())).thenThrow(new IOException("exception"));
        assertEquals(Routes.updateDruidCluster(req, res), "exception");
        verify(res, times(1)).status(500);
        when(req.params(Constants.ID)).thenReturn("!%41");
        assertEquals(Routes.updateDruidCluster(req, res), "Invalid Cluster!");
    }

    @Test
    public void testAffectedJobs() throws IOException, ClusterNotFoundException, SchedulerException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("1");
        JobMetadata j = DBTestHelper.getNewJob();
        j.setHoursOfLag(10);
        List<JobMetadata> jlist = new ArrayList<>();
        jlist.add(j);
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(jlist);
        DruidCluster dc = new DruidCluster();
        dc.setClusterId(1);
        dc.setBrokerHost("localhost");
        dc.setHoursOfLag(10);
        dc.setBrokerEndpoint("druid/v2");
        dc.setBrokerPort(1234);
        dc.setClusterName("name");
        when(dca.getDruidCluster(anyString())).thenReturn(dc);
        when(tte.render(anyObject())).thenReturn("msg");
        inject("clusterAccessor", dca);
        assertEquals(Routes.affectedJobs(req, res), "msg");
    }


    @Test
    public void testGetDatabaseJsonDump() throws IOException {
        JsonObject o = new JsonObject();
        o.addProperty("key", "val");
        JsonDumper jd = mock(JsonDumper.class);
        when(jd.getRawData()).thenReturn(o);
        inject("jsonDumper", jd);
        assertEquals(Routes.getDatabaseJsonDump(req, res), "{\"key\":\"val\"}");
        when(jd.getRawData()).thenThrow(new IOException("exception"));
        assertEquals(Routes.getDatabaseJsonDump(req, res), "exception");
    }

    @Test
    public void testWriteDatabaseJsonDump() throws IOException {
        mocks();
        JsonDumper jd = mock(JsonDumper.class);
        when(req.body()).thenReturn("{\"key\":\"val\"}");
        inject("jsonDumper", jd);
        assertEquals(Routes.writeDatabaseJsonDump(req, res), "OK");
        verify(jd, times(1)).writeRawData(any(JsonObject.class));
        verify(res, times(1)).status(200);
        doThrow(new IOException("exception")).when(jd).writeRawData(any(JsonObject.class));
        assertEquals(Routes.writeDatabaseJsonDump(req, res), "exception");
        verify(res, times(1)).status(500);
    }


    @Test
    public void testProcessInstantAnomalyJob() throws Exception {
        Routes.initParams();
        mocks();
        Query query = mock(Query.class);
        when(query.getQueryJsonObject()).thenReturn(new JsonObject());
        when(qs.build(anyString(), any(), anyInt(), anyInt(), anyInt())).thenReturn(query);
        when(req.body()).thenReturn(
            "{" +
            "\"clusterId\":\"1\"," +
            "\"granularity\":\"hour\"," +
            "\"sigmaThreshold\":\"3.5\"," +
            "\"frequency\":\"hour\"," +
            "\"queryEndTimeText\":\"2017-06-02T12:00\"," +
            "\"granularityRange\":\"1\"," +
            "\"timeseriesRange\":\"24\"," +
            "\"detectionWindow\":\"24\"," +
            "\"tsModels\":\"OlympicModel\"," +
            "\"adModels\":\"KSigmaModel\"" +
            "}"
        );
        DruidCluster dc = mock(DruidCluster.class);
        when(dca.getDruidCluster(anyString())).thenReturn(dc);
        when(dc.getHoursOfLag()).thenReturn(0);
        inject("clusterAccessor", dca);
        EgadsResult eres = mock(EgadsResult.class);
        EgadsResult.Series[] series = {
            new EgadsResult.Series(),
            new EgadsResult.Series(),
            new EgadsResult.Series()
        };
        when(eres.getData()).thenReturn(series);
        when(eres.getAnomalies()).thenReturn(Lists.newArrayList(new Anomaly()));
        List<EgadsResult> reslist = Lists.newArrayList(eres);
        when(ds.detectWithResults(any(), anyDouble(), any(), any(), any()))
                .thenReturn(reslist);
        when(tte.render(any(ModelAndView.class))).thenReturn("<div></div>");
        String a = Routes.processInstantAnomalyJob(req, res);
        verify(tte, times(1)).render(any(ModelAndView.class));
        verify(jes, times(1)).getReports(any(), any());
        when(qs.build(any(), any(), anyInt(), anyInt(), anyInt())).thenThrow(new SherlockException("error"));
        assertEquals(a, Constants.SUCCESS);
        String error = Routes.processInstantAnomalyJob(req, res);
        assertEquals(error, Constants.ERROR);
    }

    @Test
    public void testDebugInstantReport() throws Exception {
        mocks();
        DruidCluster c = mock(DruidCluster.class);
        List<DruidCluster> dclist = Collections.singletonList(c);
        when(dca.getDruidClusterList()).thenReturn(dclist);
        ModelAndView mav = Routes.debugInstantReport(req, res);
        assertEquals(mav.getViewName(), "debugForm");
        assertEquals(params(mav).get(Constants.DRUID_CLUSTERS), dclist);
    }

    @Test
    public void testDebugPowerQuery() throws Exception {
        mocks();
        QueryParamsMap qmap = mock(QueryParamsMap.class);
        Map<String, String[]> smap = new HashMap<>();
        when(qmap.toMap()).thenReturn(smap);
        when(req.queryMap()).thenReturn(qmap);
        JsonObject jo = new JsonObject();
        jo.addProperty(QueryConstants.POSTAGGREGATIONS, "a");
        jo.addProperty(QueryConstants.AGGREGATIONS, "a");
        JsonObject gran = new JsonObject();
        gran.addProperty(QueryConstants.PERIOD, "P1H");
        jo.add(QueryConstants.GRANULARITY, gran);
        jo.addProperty(QueryConstants.INTERVALS, "1");
        smap.put("query", new String[]{new Gson().toJson(jo)});
        smap.put("ownerEmail", new String[]{"jigar@mail.com,me@mail.com"});
        @SuppressWarnings("unchecked") List<AnomalyReport> arlist = mock(List.class);
        when(jes.getReports(any(), any(JobMetadata.class))).thenReturn(arlist);
        when(tte.render(any(ModelAndView.class))).thenReturn("html");
        doAnswer(iom -> {
                JobMetadata j = (JobMetadata) iom.getArguments()[0];
                j.setJobId(12);
                return null;
            }
        ).when(jma).putJobMetadata(any(JobMetadata.class));
        ModelAndView mav = Routes.debugPowerQuery(req, res);
        assertEquals(params(mav).get("tableHtml"), "html");
        verify(jes, times(1)).getReports(any(), any());
        verify(jes, times(1)).executeJob(any(JobMetadata.class), any(DruidCluster.class), any(Query.class));
    }

    @Test
    public void testDebugPowerQueryException() throws Exception {
        mocks();
        @SuppressWarnings("unchecked") List<AnomalyReport> arlist = mock(List.class);
        when(jes.getReports(any(), any(JobMetadata.class))).thenReturn(arlist);
        doAnswer(iom -> {
                JobMetadata j = (JobMetadata) iom.getArguments()[0];
                j.setJobId(12);
                return null;
            }
        ).when(jma).putJobMetadata(any(JobMetadata.class));
        QueryParamsMap qmap = mock(QueryParamsMap.class);
        Map<String, String[]> smap = new HashMap<>();
        smap.put("query", new String[]{""});
        smap.put("ownerEmail", new String[]{"jigar@mail.com,me@mail.com"});
        when(qmap.toMap()).thenReturn(smap);
        when(req.queryMap()).thenReturn(qmap);
        ModelAndView mav = Routes.debugPowerQuery(req, res);
        assertEquals(params(mav).get(Constants.ERROR), "Empty query string provided");
    }

    @Test
    public void testDebugBackfillForm() throws Exception {
        @SuppressWarnings("unchecked")
        List<JobMetadata> jmlist = new ArrayList<>();
        mocks();
        when(jma.getJobMetadataList()).thenReturn(jmlist);
        ModelAndView mav = Routes.debugBackfillForm(req, res);
        assertEquals(params(mav).get("jobs"), jmlist);
    }

    @Test
    public void testDebugRunBackfillJob() throws Exception {
        mocks();
        when(req.body()).thenReturn("{\"query\":\"{}\",\"granularity\":\"day\",\"jobId\":\"1,2\",\"fillStartTime\":\"2017-10-08T02:00\"}");
        JobMetadata jm = mock(JobMetadata.class);
        when(jma.getJobMetadata(anyString())).thenReturn(jm);
        when(jm.getHoursOfLag()).thenReturn(0);
        when(jm.getGranularity()).thenReturn("day");
        when(jm.getClusterId()).thenReturn(1);
        when(jm.getGranularityRange()).thenReturn(1);
        TestUtilities.inject(jes, JobExecutionService.class, "druidClusterAccessor", dca);
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_2.json")));
        when(jm.getUserQuery()).thenReturn(queryString);
        DruidCluster dc = mock(DruidCluster.class);
        when(dca.getDruidCluster(anyString())).thenReturn(dc);
        doCallRealMethod().when(jes).performBackfillJob(any(), any(), any());
        assertEquals(Routes.debugRunBackfillJob(req, res), "Success");
        verify(dca, times(2)).getDruidCluster(anyInt());
        verify(jma, times(2)).getJobMetadata(anyString());
        verify(jes, times(2)).performBackfillJob(any(), any(), any(), anyInt(), anyInt(), any(), anyInt());
    }

    @Test
    public void testDebugRunBackfillJobException() throws Exception {
        mocks();
        when(req.body()).thenReturn("{\"query\":\"{}\",\"granularity\":\"day\",\"jobId\":\"1,2\"}");
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("error"));
        assertEquals(Routes.debugRunBackfillJob(req, res), "error");
    }

    @Test
    public void testDebugClearJobReports() throws Exception {
        mocks();
        when(req.params(anyString())).thenReturn("123");
        assertEquals(Routes.debugClearJobReports(req, res), "Success");
        verify(ara, times(1)).deleteAnomalyReportsForJob("123");
    }

    @Test
    public void testDebugClearJobReportsException() throws Exception {
        mocks();
        doThrow(new IOException("error")).when(ara).deleteAnomalyReportsForJob(anyString());
        assertEquals("error", Routes.debugClearJobReports(req, res));
    }

    @Test
    public void testDebugClearDebugJobs() throws Exception {
        mocks();
        assertEquals("Success", Routes.debugClearDebugJobs(req, res));
        verify(jma, times(1)).deleteDebugJobs();
    }

    @Test
    public void testDebugClearDebugJobsException() throws Exception {
        mocks();
        doThrow(new IOException("error")).when(jma).deleteDebugJobs();
        assertEquals("error", Routes.debugClearDebugJobs(req, res));
    }

    @Test
    public void testDebugEgadsQueryView() throws IOException {
        @SuppressWarnings("unchecked") List<DruidCluster> dclist = mock(List.class);
        mocks();
        when(dca.getDruidClusterList()).thenReturn(dclist);
        ModelAndView mav = Routes.debugShowEgadsConfigurableQuery(req, res);
        assertEquals(params(mav).get(Constants.DRUID_CLUSTERS), dclist);
        verify(dca, times(1)).getDruidClusterList();
        Assert.assertEquals(params(mav).get("filteringMethods"), EgadsConfig.FilteringMethod.values());
    }

    @Test
    public void testDebugPerformEgadsQuery() throws Exception {
        mocks();
        JsonObject jo = new JsonObject();
        jo.addProperty(QueryConstants.POSTAGGREGATIONS, "a");
        jo.addProperty(QueryConstants.AGGREGATIONS, "a");
        JsonObject gran = new JsonObject();
        gran.addProperty(QueryConstants.PERIOD, "P1H");
        jo.add(QueryConstants.GRANULARITY, gran);
        jo.addProperty(QueryConstants.INTERVALS, "1");
        QueryParamsMap map = mock(QueryParamsMap.class);
        Map<String, String[]> smap = new HashMap<>();
        when(map.toMap()).thenReturn(smap);
        when(req.queryMap()).thenReturn(map);
        smap.put("query", new String[]{new Gson().toJson(jo)});
        smap.put("ownerEmail", new String[]{"jigar@mail.com,me@mail.com"});
        EgadsResult er = mock(EgadsResult.class);
        EgadsResult.Series series = mock(EgadsResult.Series.class);
        EgadsResult.Series[] data = {series, series, series};
        when(er.getData()).thenReturn(data);
        List<Anomaly> anomalies = Lists.newArrayList(new Anomaly());
        when(er.getAnomalies()).thenReturn(anomalies);
        List<EgadsResult> erlist = Lists.newArrayList(er);
        when(tte.render(any(ModelAndView.class))).thenReturn("html");
        when(ds.detectWithResults(any(), anyDouble(), any(), anyInt(), any())).thenReturn(erlist);
        ModelAndView mav = Routes.debugPerformEgadsQuery(req, res);
        assertTrue(params(mav).containsKey("tableHtml"));
        assertEquals(params(mav).get("tableHtml"), "html");
        assertTrue(params(mav).containsKey("data"));
        verify(ds, times(1)).detectWithResults(any(), anyDouble(), any(), anyInt(), any());
        verify(jes, times(1)).getReports(any(), any());
    }

    @Test
    public void testCloneJob() throws Exception {
        mocks();
        JobMetadata jm = new JobMetadata();
        when(req.params(Constants.ID)).thenReturn("1");
        inject("jobAccessor", jma);
        jm.setJobId(1);
        jm.setTestName("test1");
        jm.setFrequency("day");
        when(jma.getJobMetadata(anyString())).thenReturn(jm);
        doAnswer(iom -> {
                Object[] args = iom.getArguments();
                ((JobMetadata) args[0]).setJobId(10);
                assertEquals(((JobMetadata) args[0]).getJobId(), (Integer) 10);
                assertEquals(((JobMetadata) args[0]).getTestName(), "test1_cloned");
                return ((JobMetadata) args[0]).getJobId().toString();
            }
        ).when(jma).putJobMetadata(any(JobMetadata.class));
        assertEquals(Routes.cloneJob(req, res), "10");
        verify(res, times(1)).status(200);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("clone error"));
        assertEquals(Routes.cloneJob(req, res), "clone error");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testRerunJob() throws Exception {
        mocks();
        ServiceFactory sf = mock(ServiceFactory.class);
        JobMetadata jm = new JobMetadata();
        when(req.body()).thenReturn("{\"query\":\"{}\",\"granularity\":\"day\",\"jobId\":\"2\",\"timestamp\":\"20000000\"}");
        inject("jobAccessor", jma);
        inject("serviceFactory", sf);
        jm.setJobId(2);
        jm.setGranularity("day");
        when(jma.getJobMetadata(anyString())).thenReturn(jm);
        when(sf.newJobExecutionService()).thenReturn(jes);
        doNothing().when(jes).performBackfillJob(any(JobMetadata.class), any(ZonedDateTime.class), any(ZonedDateTime.class));
        assertEquals(Routes.rerunJob(req, res), "success");
        verify(jma, times(1)).getJobMetadata(anyString());
        verify(jes, times(1)).performBackfillJob(any(JobMetadata.class), any(ZonedDateTime.class), any(ZonedDateTime.class));
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("rerun error"));
        assertEquals(Routes.rerunJob(req, res), "rerun error");
        verify(res, times(1)).status(500);
        when(req.body()).thenReturn("{\"query\":\"{}\",\"granularity\":\"day\",\"jobId\":\"%3c%73%\",\"timestamp\":\"20000000\"}");
        assertEquals(Routes.rerunJob(req, res), "Invalid Job!");
    }

    @Test
    public void testClearReportsOfSelectedJobs() throws Exception {
        mocks();
        when(req.params(anyString())).thenReturn("1,2,3");
        assertEquals(Routes.clearReportsOfSelectedJobs(req, res), "success");
        verify(ara, times(3)).deleteAnomalyReportsForJob(anyString());
        doThrow(new IOException("io error")).when(ara).deleteAnomalyReportsForJob("2");
        assertEquals(Routes.clearReportsOfSelectedJobs(req, res), "io error");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testLaunchSelectedJobs() throws Exception {
        mocks();
        SchedulerService ss = mock(SchedulerService.class);
        inject("schedulerService", ss);
        when(req.params(anyString())).thenReturn("1,2,3");
        JobMetadata job1 = DBTestHelper.getNewJob(); job1.setJobId(1); job1.setJobStatus(JobStatus.RUNNING.getValue());
        JobMetadata job2 = DBTestHelper.getNewJob(); job2.setJobId(2); job2.setJobStatus(JobStatus.NODATA.getValue());
        JobMetadata job3 = DBTestHelper.getNewJob(); job3.setJobId(3); job3.setJobStatus(JobStatus.CREATED.getValue());
        when(jma.getJobMetadata("1")).thenReturn(job1);
        when(jma.getJobMetadata("2")).thenReturn(job2);
        when(jma.getJobMetadata("3")).thenReturn(job3);
        DruidCluster cluster = DBTestHelper.getNewDruidCluster();
        when(dca.getDruidCluster(anyInt())).thenReturn(cluster);
        doNothing().when(ss).scheduleJob(any(JobMetadata.class));
        when(jma.putJobMetadata(any(JobMetadata.class))).thenReturn("");
        assertEquals(Routes.launchSelectedJobs(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(jma, times(1)).putJobMetadata(any(JobMetadata.class));
        verify(ss, times(1)).scheduleJob(any(JobMetadata.class));
        assertEquals(job3.getJobStatus(), "RUNNING");
        job3.setJobStatus("CREATED");
        when(jma.putJobMetadata(any(JobMetadata.class))).thenThrow(new IOException("io error"));
        assertEquals(Routes.launchSelectedJobs(req, res), "io error");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testStopSelectedJobs() throws Exception {
        mocks();
        SchedulerService ss = mock(SchedulerService.class);
        inject("schedulerService", ss);
        when(req.params(anyString())).thenReturn("1,2,3");
        JobMetadata job1 = DBTestHelper.getNewJob(); job1.setJobId(1); job1.setJobStatus(JobStatus.RUNNING.getValue());
        JobMetadata job2 = DBTestHelper.getNewJob(); job2.setJobId(2); job2.setJobStatus(JobStatus.NODATA.getValue());
        JobMetadata job3 = DBTestHelper.getNewJob(); job3.setJobId(3); job3.setJobStatus(JobStatus.STOPPED.getValue());
        when(jma.getJobMetadata("1")).thenReturn(job1);
        when(jma.getJobMetadata("2")).thenReturn(job2);
        when(jma.getJobMetadata("3")).thenReturn(job3);
        doNothing().when(ss).stopJob(anyInt());
        when(jma.putJobMetadata(any(JobMetadata.class))).thenReturn("");
        assertEquals(Routes.stopSelectedJobs(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(jma, times(3)).putJobMetadata(any(JobMetadata.class));
        verify(ss, times(3)).stopJob(anyInt());
        assertEquals(job3.getJobStatus(), "STOPPED");
        assertEquals(job2.getJobStatus(), "STOPPED");
        assertEquals(job1.getJobStatus(), "STOPPED");
        when(jma.putJobMetadata(any(JobMetadata.class))).thenThrow(new IOException("io error"));
        assertEquals(Routes.stopSelectedJobs(req, res), "io error");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testdeleteSelectedJobs() throws Exception {
        mocks();
        SchedulerService ss = mock(SchedulerService.class);
        inject("schedulerService", ss);
        when(req.params(anyString())).thenReturn("1,2,3");
        Set<String> jobSet = new HashSet<String>() {
            {
                add("1");
                add("2");
                add("3");
            }
        };
        doNothing().when(jma).deleteJobs(jobSet);
        doNothing().when(ss).stopJob(jobSet);
        assertEquals(Routes.deleteSelectedJobs(req, res), Constants.SUCCESS);
        verify(jma, times(1)).deleteJobs(jobSet);
        verify(ss, times(1)).stopJob(jobSet);
        doThrow(new IOException("io error")).when(jma).deleteJobs(jobSet);
        assertEquals(Routes.deleteSelectedJobs(req, res), "io error");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testUpdateEmail() throws IOException, EmailNotFoundException {
        mocks();
        when(req.body()).thenReturn(
            "{" +
            "\"emailId\":\"my@email.com\"," +
            "\"sendOutHour\":\"23\"," +
            "\"sendOutMinute\":\"54\"," +
            "\"repeatInterval\":\"day\"" +
            "}"
        );
        when(ema.getEmailMetadata("my@email.com")).thenReturn(new EmailMetaData("my@email.com"));
        doNothing().when(ema).removeFromTriggerIndex(anyString(), anyString());
        doNothing().when(ema).putEmailMetadata(any(EmailMetaData.class));
        assertEquals(Routes.updateEmails(req, res), Constants.SUCCESS);
        verify(ema, times(1)).removeFromTriggerIndex(anyString(), anyString());
        when(req.body()).thenReturn(
            "{" +
            "\"emailId\":\"invalid.com\"," +
            "\"sendOutHour\":\"23\"," +
            "\"sendOutMinute\":\"54\"," +
            "\"repeatInterval\":\"day\"" +
            "}"
        );
        assertEquals(Routes.updateEmails(req, res), "Invalid Email!");
    }

    @Test
    public void testViewEmail() throws IOException, EmailNotFoundException {
        mocks();
        when(req.params(Constants.ID)).thenReturn("my@email.com");
        when(ema.getEmailMetadata(anyString())).thenReturn(new EmailMetaData("my@email.com"));
        ModelAndView mav = Routes.viewEmails(req, res);
        assertEquals(mav.getViewName(), "emailInfo");
    }

    @Test
    public void testDeleteEmail() throws IOException, EmailNotFoundException {
        mocks();
        when(req.body()).thenReturn("{\"emailId\":\"xyz@abc.com\"}");
        when(ema.getEmailMetadata(anyString())).thenReturn(new EmailMetaData("my@email.com"));
        doNothing().when(jma).deleteEmailFromJobs(any(EmailMetaData.class));
        String result = Routes.deleteEmail(req, res);
        assertEquals(result, Constants.SUCCESS);
        // test exception
        doThrow(new IOException("error")).when(jma).deleteEmailFromJobs(any(EmailMetaData.class));
        result = Routes.deleteEmail(req, res);
        assertEquals(result, Constants.ERROR);
        when(req.body()).thenReturn("{\"emailId\":\"xyzabc.com\"}");
        result = Routes.deleteEmail(req, res);
        assertEquals(result, "Invalid Email!");
    }

    @Test
    public void testRestoreRedisDBForm() throws IOException, EmailNotFoundException {
        mocks();
        ModelAndView mav = Routes.restoreRedisDBForm(req, res);
        assertEquals(mav.getViewName(), "redisRestoreForm");
    }

    @Test
    public void testRestoreRedisDB() throws IOException, EmailNotFoundException, SchedulerException {
        mocks();
        when(req.body()).thenReturn("{\"path\":\"//path//test//file//dump.json\"}");
        SchedulerService ss = mock(SchedulerService.class);
        inject("schedulerService", ss);
        JsonDumper jd = mock(JsonDumper.class);
        inject("jsonDumper", jd);
        doNothing().when(jd).writeRawData(any());
        doNothing().when(ss).removeAllJobsFromQueue();
        String response = Routes.restoreRedisDB(req, res);
        assertNotEquals(response, Constants.SUCCESS);
    }

    @Test
    public void testBuildIndexes() throws IOException, JobNotFoundException {
        mocks();
        Set<String> jobSet = new HashSet<String>() {
            {
                add("1");
                add("2");
                add("3");
            }
        };
        Set<String> clusterSet = new HashSet<String>() {
            {
                add("1");
                add("2");
            }
        };
        Set<String> emailSet = new HashSet<String>() {
            {
                add("email1");
                add("email2");
            }
        };
        JsonDumper jd = mock(JsonDumper.class);
        inject("jsonDumper", jd);
        when(jma.getJobIds()).thenReturn(jobSet);
        when(dca.getDruidClusterIds()).thenReturn(clusterSet);
        when(ema.getAllEmailIds()).thenReturn(emailSet);
        when(jma.getJobMetadata("1")).thenReturn(DBTestHelper.getNewJob());
        when(jma.getJobMetadata("2")).thenThrow(JobNotFoundException.class);
        when(jma.getJobMetadata("3")).thenReturn(DBTestHelper.getNewJob());
        doNothing().when(jd).clearIndexes(anyString(), anyString());
        when(jma.putJobMetadata(any(JobMetadata.class))).thenReturn("");
        doNothing().when(jma).removeFromJobIdIndex(anyString());
        String response = Routes.buildIndexes(req, res);
        assertEquals(response, Constants.SUCCESS);
        verify(jma, times(2)).putJobMetadata(any(JobMetadata.class));
        verify(jma, times(1)).removeFromJobIdIndex(anyString());
    }
}
