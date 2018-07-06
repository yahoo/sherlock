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
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.scheduler.JobExecutionService;
import com.yahoo.sherlock.scheduler.SchedulerService;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.DruidQueryService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.settings.CLISettingsTest;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.QueryConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JsonDumper;
import org.testng.Assert;
import org.testng.annotations.Test;

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
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class RoutesTest {

    private Request fRequest = mock(Request.class);
    private Response fResponse = mock(Response.class);

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
        CLISettingsTest.setField(CLISettingsTest.getField("jsonDumper", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("serviceFactory", Routes.class), null);
        CLISettingsTest.setField(CLISettingsTest.getField("schedulerService", Routes.class), null);
        try {
            Routes.initServices();
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("reportAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("deletedJobAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("clusterAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("jobAccessor", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("jsonDumper", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("serviceFactory", Routes.class)));
            assertNotNull(CLISettingsTest.fieldVal(CLISettingsTest.getField("schedulerService", Routes.class)));
        } finally {
            CLISettingsTest.setField(CLISettingsTest.getField("reportAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("deletedJobAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("clusterAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("jobAccessor", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("jsonDumper", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("serviceFactory", Routes.class), null);
            CLISettingsTest.setField(CLISettingsTest.getField("schedulerService", Routes.class), null);
        }
    }

    @Test
    public void testViewHomePage() {
        Routes.initParams();
        ModelAndView mav = Routes.viewHomePage(fRequest, fResponse);
        assertEquals(mav.getViewName(), "homePage");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewInstantAnomalyFormEmptyDruidList() throws IOException {
        Routes.initParams();
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        when(dca.getDruidClusterList()).thenThrow(new IOException());
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewInstantAnomalyJobForm(fRequest, fResponse);
        assertEquals(params(mav.getModel()).get(Constants.INSTANTVIEW), "true");
        List<DruidCluster> clusters = (List<DruidCluster>) params(mav.getModel()).get(Constants.DRUID_CLUSTERS);
        assertEquals(clusters.size(), 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewInstantAnomalyForm() throws IOException {
        Routes.initParams();
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        DruidCluster cl = new DruidCluster();
        cl.setClusterId(5);
        List<DruidCluster> cls = Collections.singletonList(cl);
        when(dca.getDruidClusterList()).thenReturn(cls);
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewInstantAnomalyJobForm(fRequest, fResponse);
        List<DruidCluster> clusters = (List<DruidCluster>) params(mav.getModel()).get(Constants.DRUID_CLUSTERS);
        assertEquals(clusters.size(), 1);
        assertEquals(clusters.get(0).getClusterId(), (Integer) 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewNewAnomalyForm() throws IOException {
        Routes.initParams();
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        DruidCluster cl = new DruidCluster();
        cl.setClusterId(5);
        List<DruidCluster> cls = Collections.singletonList(cl);
        when(dca.getDruidClusterList()).thenReturn(cls);
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewNewAnomalyJobForm(fRequest, fResponse);
        List<DruidCluster> clusters = (List<DruidCluster>) params(mav.getModel()).get(Constants.DRUID_CLUSTERS);
        assertEquals(clusters.size(), 1);
        assertEquals(clusters.get(0).getClusterId(), (Integer) 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewNewAnomalyFormEmptyDruidList() throws IOException {
        Routes.initParams();
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        when(dca.getDruidClusterList()).thenThrow(new IOException());
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewNewAnomalyJobForm(fRequest, fResponse);
        assertTrue(params(mav.getModel()).containsKey(Constants.ERROR));
    }

    @Test
    public void testSaveUserJob() throws Exception {
        Request req = mock(Request.class);
        Response res = mock(Response.class);
        when(req.body()).thenReturn(
            "{" +
            "\"clusterId\":\"1\"," +
            "\"ownerEmail\":\"someone@something.com\"" +
            "}"
        );
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        ServiceFactory sf = mock(ServiceFactory.class);
        when(sf.newEmailServiceInstance()).thenCallRealMethod();
        DruidQueryService dqs = mock(DruidQueryService.class);
        Query query = mock(Query.class);
        JsonObject jo = new JsonObject();
        when(query.getQueryJsonObject()).thenReturn(jo);
        when(dqs.build(anyString(), any(Granularity.class), anyInt(), anyInt())).thenReturn(query);
        when(sf.newDruidQueryServiceInstance()).thenReturn(dqs);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        DruidCluster dc = mock(DruidCluster.class);
        when(dca.getDruidCluster(anyInt())).thenReturn(dc);
        when(dc.getHoursOfLag()).thenReturn(0);
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
        Request req = mock(Request.class);
        Response res = mock(Response.class);
        when(req.body()).thenReturn(
                "{" +
                        "\"clusterId\":\"1\"," +
                        "\"ownerEmail\":\"someone@something.com\"" +
                        "}"
        );
        ServiceFactory sf = mock(ServiceFactory.class);
        when(sf.newEmailServiceInstance()).thenCallRealMethod();
        DruidQueryService dqs = mock(DruidQueryService.class);
        when(dqs.build(anyString(), any(Granularity.class), anyInt(), anyInt())).thenThrow(new SherlockException("exception"));
        when(sf.newDruidQueryServiceInstance()).thenReturn(dqs);
        inject("serviceFactory", sf);
        assertEquals(Routes.saveUserJob(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testDeleteJob() throws SchedulerException {
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        SchedulerService ss = mock(SchedulerService.class);
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        inject("jobAccessor", jma);
        inject("schedulerService", ss);
        assertEquals(Routes.deleteJob(req, fResponse), Constants.SUCCESS);
        verify(ss, times(1)).stopJob(1);
    }

    @Test
    public void testDeleteJobException() throws IOException, JobNotFoundException {
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        SchedulerService ss = mock(SchedulerService.class);
        inject("schedulerService", ss);
        doThrow(new IOException("fake")).when(jma).deleteJobMetadata(anyInt());
        inject("jobAccessor", jma);
        Response res = mock(Response.class);
        assertEquals(Routes.deleteJob(fRequest, res), "fake");
        verify(res, times(1)).status(500);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewJobsList() throws IOException {
        Routes.initParams();
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        List<JobMetadata> jobsList = (List<JobMetadata>) mock(List.class);
        when(jobsList.size()).thenReturn(1);
        when(jma.getJobMetadataList()).thenReturn(jobsList);
        inject("jobAccessor", jma);
        ModelAndView mav = Routes.viewJobsList(fRequest, fResponse);
        assertTrue(params(mav).containsKey(Constants.TITLE));
        assertEquals(((List) params(mav).get("jobs")).size(), 1);
        assertEquals(mav.getViewName(), "listJobs");
    }

    @Test
    public void testViewJobsListException() throws IOException {
        Routes.initParams();
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        when(jma.getJobMetadataList()).thenThrow(new IOException("exception"));
        inject("jobAccessor", jma);
        ModelAndView mav = Routes.viewJobsList(fRequest, fResponse);
        assertTrue(params(mav).containsKey(Constants.ERROR));
        assertEquals(params(mav).get(Constants.ERROR), "exception");
        assertEquals(mav.getViewName(), "listJobs");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testViewDeletedJobsList() throws IOException {
        Routes.initParams();
        List<JobMetadata> ljm = (List<JobMetadata>) mock(List.class);
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        inject("deletedJobAccessor", dma);
        when(dma.getDeletedJobMetadataList()).thenReturn(ljm);
        ModelAndView mav = Routes.viewDeletedJobsList(fRequest, fResponse);
        assertTrue(params(mav).containsKey(Constants.TITLE));
        assertEquals(params(mav).get(Constants.DELETEDJOBSVIEW), "true");
        assertEquals(mav.getViewName(), "listJobs");
        assertEquals(params(mav).get("jobs"), ljm);
    }

    @Test
    public void testDeletedJobsListException() throws IOException {
        Routes.initParams();
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        inject("deletedJobAccessor", dma);
        when(dma.getDeletedJobMetadataList()).thenThrow(new IOException("exception"));
        ModelAndView mav = Routes.viewDeletedJobsList(fRequest, fResponse);
        assertTrue(params(mav).containsKey(Constants.ERROR));
    }

    @Test
    public void testViewJobInfo() throws IOException, JobNotFoundException {
        Routes.initParams();
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        JobMetadata jm = mock(JobMetadata.class);
        when(jma.getJobMetadata("1")).thenReturn(jm);
        inject("jobAccessor", jma);
        ModelAndView mav = Routes.viewJobInfo(req, fResponse);
        assertTrue(params(mav).containsKey(Constants.TITLE));
        assertEquals(params(mav).get("job"), jm);
    }

    @Test
    public void testViewJobInfoException() throws IOException, JobNotFoundException {
        Routes.initParams();
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        inject("jobAccessor", jma);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("fake"));
        ModelAndView mav = Routes.viewJobInfo(fRequest, fResponse);
        assertEquals(params(mav).get(Constants.ERROR), "fake");
    }

    @Test
    public void testViewDeletedJobInfo() throws IOException, JobNotFoundException, ClusterNotFoundException {
        Routes.initParams();
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        when(dca.getDruidCluster(anyInt())).thenReturn(new DruidCluster());
        inject("clusterAccessor", dca);
        JobMetadata jm = mock(JobMetadata.class);
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        when(dma.getDeletedJobMetadata("1")).thenReturn(jm);
        inject("deletedJobAccessor", dma);
        ModelAndView mav = Routes.viewDeletedJobInfo(req, fResponse);
        assertEquals(params(mav).get(Constants.DELETEDJOBSVIEW), "true");
        assertEquals(params(mav).get("job"), jm);
    }

    @Test
    public void testViewDeletedJobInfoException() throws IOException, JobNotFoundException {
        Routes.initParams();
        DeletedJobMetadataAccessor dma = mock(DeletedJobMetadataAccessor.class);
        when(dma.getDeletedJobMetadata(anyString())).thenThrow(new IOException("fake"));
        inject("deletedJobAccessor", dma);
        ModelAndView mav = Routes.viewDeletedJobInfo(fRequest, fResponse);
        assertEquals(params(mav).get(Constants.ERROR), "fake");
    }

    @Test
    public void testUpdateJobInfo() throws Exception {
        String body = "{\"granularity\":\"day\",\"frequency\":\"day\",\"sigmaThreshold\":\"3\",\"ownerEmail\":\"someone@something.com\",\"query\":\"{}\"}";
        Request req = mock(Request.class);
        when(req.body()).thenReturn(body);
        when(req.params(Constants.ID)).thenReturn("1");
        Response res = mock(Response.class);
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        JobMetadata jm = new JobMetadata();
        jm.setJobId(1);
        jm.setGranularity("day");
        jm.setFrequency("day");
        jm.setSigmaThreshold(3.0);
        jm.setJobStatus("RUNNING");
        jm.setUserQuery("query");
        jm.setQuery("query");
        when(jma.getJobMetadata("1")).thenReturn(jm);
        SchedulerService ss = mock(SchedulerService.class);
        ServiceFactory sf = mock(ServiceFactory.class);
        DruidQueryService dqs = mock(DruidQueryService.class);
        Query q = mock(Query.class);
        JsonObject j = new JsonObject();
        when(sf.newEmailServiceInstance()).thenCallRealMethod();
        when(q.getQueryJsonObject()).thenReturn(j);
        when(dqs.build(anyString(), any(Granularity.class), anyInt(), anyInt())).thenReturn(q);
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
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("exception"));
        Request req = mock(Request.class);
        when(req.body()).thenReturn("{\"ownerEmail\":\"someone@something.com\"}");
        inject("jobAccessor", jma);
        ServiceFactory sf = new ServiceFactory();
        inject("serviceFactory", sf);
        Response res = mock(Response.class);
        assertEquals(Routes.updateJobInfo(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testLaunchJob() throws IOException, JobNotFoundException, SchedulerException, ClusterNotFoundException {
        mocks();
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        JobMetadata jm = new JobMetadata();
        SchedulerService ss = mock(SchedulerService.class);
        inject("jobAccessor", jma);
        inject("schedulerService", ss);
        Request req = mock(Request.class);
        Response res = mock(Response.class);
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
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        inject("jobAccessor", jma);
        Response res = mock(Response.class);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("exception"));
        assertEquals(Routes.launchJob(fRequest, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testStopJob() throws IOException, JobNotFoundException, SchedulerException {
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        JobMetadata jm = new JobMetadata();
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        Response res = mock(Response.class);
        inject("jobAccessor", jma);
        jm.setJobId(1);
        jm.setFrequency("day");
        SchedulerService ss = mock(SchedulerService.class);
        inject("schedulerService", ss);
        when(jma.getJobMetadata("1")).thenReturn(jm);
        assertEquals(Routes.stopJob(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(jma, times(1)).putJobMetadata(jm);
        verify(ss, times(1)).stopJob(1);
    }

    @Test
    public void testStopJobException() throws IOException, JobNotFoundException {
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        when(jma.getJobMetadata(anyString())).thenThrow(new IOException("exception"));
        Response res = mock(Response.class);
        inject("jobAccessor", jma);
        assertEquals(Routes.stopJob(fRequest, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testViewJobReport() throws Exception {
        Routes.initParams();
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        when(req.params(Constants.FREQUENCY_PARAM)).thenReturn("day");
        AnomalyReportAccessor ara = mock(AnomalyReportAccessor.class);
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        JobMetadata jm = new JobMetadata();
        List<AnomalyReport> lar = Collections.emptyList();
        when(ara.getAnomalyReportsForJob(anyString(), anyString())).thenReturn(lar);
        inject("reportAccessor", ara);
        inject("jobAccessor", jma);
        when(jma.getJobMetadata(anyString())).thenReturn(jm);
        ModelAndView mav = Routes.viewJobReport(req, fResponse);
        assertEquals(mav.getViewName(), "report");
        assertEquals(params(mav).get(Constants.FREQUENCY), "day");
        assertTrue(params(mav).containsKey(Constants.TIMELINE_POINTS));
    }

    @Test
    public void testSendJobReport() throws IOException {
        Routes.initParams();
        ThymeleafTemplateEngine tte = mock(ThymeleafTemplateEngine.class);
        when(tte.render(any(ModelAndView.class))).thenReturn("table");
        Response res = mock(Response.class);
        AnomalyReportAccessor ara = mock(AnomalyReportAccessor.class);
        AnomalyReport a = mock(AnomalyReport.class);
        List<AnomalyReport> lar = Collections.singletonList(a);
        when(ara.getAnomalyReportsForJobAtTime(anyString(), anyString(), anyString())).thenReturn(lar);
        Request req = mock(Request.class);
        when(req.body()).thenReturn("{\"" + Constants.SELECTED_DATE + "\":\"5678\"}");
        inject("thymeleaf", tte);
        inject("reportAccessor", ara);
        assertEquals(Routes.sendJobReport(req, res), "table");
        verify(res, times(1)).status(200);
    }

    @Test
    public void testSendJobReportException() throws IOException {
        Routes.initParams();
        AnomalyReportAccessor ara = mock(AnomalyReportAccessor.class);
        when(ara.getAnomalyReportsForJobAtTime(anyString(), anyString(), anyString())).thenThrow(new IOException("exception"));
        Request req = mock(Request.class);
        inject("reportAccessor", ara);
        when(req.body()).thenReturn("{}");
        Response res = mock(Response.class);
        assertTrue(Routes.sendJobReport(req, res).contains("exception"));
        verify(res, times(1)).status(500);
    }

    @Test
    public void testViewNewDruidClusterForm() {
        Routes.initParams();
        ModelAndView mav = Routes.viewNewDruidClusterForm(fRequest, fResponse);
        assertEquals(mav.getViewName(), "druidForm");
    }

    @Test
    public void testAddNewDruidCluster() throws IOException {
        String body =
                "{" +
                        "\"clusterName\":\"name\"," +
                        "\"brokerHost\":\"localhost\"," +
                        "\"brokerPort\":\"5080\"," +
                        "\"brokerEndpoint\":\"druid/v2\"" +
                        "}";
        Request req = mock(Request.class);
        when(req.body()).thenReturn(body);
        Response res = mock(Response.class);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
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
        String body = "{}";
        Request req = mock(Request.class);
        when(req.body()).thenReturn(body);
        Response res = mock(Response.class);
        assertEquals(Routes.addNewDruidCluster(req, res), "Cluster name cannot be empty");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testAddNewClusterException() throws IOException {
        String body =
                "{" +
                        "\"clusterName\":\"name\"," +
                        "\"brokerHost\":\"localhost\"," +
                        "\"brokerPort\":\"5080\"," +
                        "\"brokerEndpoint\":\"druid/v2\"" +
                        "}";
        Request req = mock(Request.class);
        when(req.body()).thenReturn(body);
        Response res = mock(Response.class);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        inject("clusterAccessor", dca);
        doThrow(new IOException("exception")).when(dca).putDruidCluster(any(DruidCluster.class));
        assertEquals(Routes.addNewDruidCluster(req, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testViewDruidClusterList() throws IOException {
        Routes.initParams();
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        inject("clusterAccessor", dca);
        when(dca.getDruidClusterList()).thenReturn(Collections.emptyList());
        ModelAndView mav = Routes.viewDruidClusterList(fRequest, fResponse);
        assertEquals(mav.getViewName(), "druidList");
        assertEquals(((List) params(mav).get("clusters")).size(), 0);
    }

    @Test
    public void testViewDruidClusterListException() throws IOException {
        Routes.initParams();
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        inject("clusterAccessor", dca);
        when(dca.getDruidClusterList()).thenThrow(new IOException("exception"));
        ModelAndView mav = Routes.viewDruidClusterList(fRequest, fResponse);
        assertEquals(params(mav).get(Constants.ERROR), "exception");
    }

    @Test
    public void testViewDruidCluster() throws IOException, ClusterNotFoundException {
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        DruidCluster dc = new DruidCluster();
        dc.setClusterId(1);
        when(dca.getDruidCluster("1")).thenReturn(dc);
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewDruidCluster(req, fResponse);
        assertEquals(mav.getViewName(), "druidInfo");
        assertEquals(((DruidCluster) params(mav).get("cluster")).getClusterId(), (Integer) 1);
    }

    @Test
    public void testViewDruidClusterException() throws IOException, ClusterNotFoundException {
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        when(dca.getDruidCluster(anyString())).thenThrow(new IOException("exception"));
        inject("clusterAccessor", dca);
        ModelAndView mav = Routes.viewDruidCluster(fRequest, fResponse);
        assertEquals(params(mav).get(Constants.ERROR), "exception");
    }

    @Test
    public void testDeleteDruidCluster() throws IOException, ClusterNotFoundException {
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(Collections.emptyList());
        inject("jobAccessor", jma);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        inject("clusterAccessor", dca);
        Response res = mock(Response.class);
        assertEquals(Routes.deleteDruidCluster(req, res), Constants.SUCCESS);
        verify(res, times(1)).status(200);
        verify(dca, times(1)).deleteDruidCluster("1");
    }

    @Test
    public void testDeleteDruidClusterAssociatedJobs() throws IOException {
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        inject("jobAccessor", jma);
        JobMetadata jm = new JobMetadata();
        jm.setJobId(2);
        List<JobMetadata> jlist = Collections.singletonList(jm);
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(jlist);
        Response res = mock(Response.class);
        assertEquals(
                Routes.deleteDruidCluster(fRequest, res),
                "Cannot delete cluster with 1 associated jobs"
        );
        verify(res, times(1)).status(400);
    }

    @Test
    public void testDeleteDruidClusterException() throws IOException, ClusterNotFoundException {
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        when(jma.getJobsAssociatedWithCluster(anyString())).thenReturn(Collections.emptyList());
        doThrow(new IOException("exception")).when(dca).deleteDruidCluster(anyString());
        inject("jobAccessor", jma);
        inject("clusterAccessor", dca);
        Response res = mock(Response.class);
        assertEquals(Routes.deleteDruidCluster(fRequest, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testUpdateDruidCluster() throws IOException, ClusterNotFoundException {
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        JsonObject dcjson = new JsonObject();
        dcjson.addProperty("clusterName", "name");
        dcjson.addProperty("clusterDescription", "descr");
        dcjson.addProperty("brokerHost", "hostname");
        dcjson.addProperty("brokerPort", "431");
        dcjson.addProperty("brokerEndpoint", "druid/v2");
        String body = new Gson().toJson(dcjson);
        when(req.body()).thenReturn(body);
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        DruidCluster dc = new DruidCluster();
        dc.setClusterId(1);
        dc.setBrokerHost("localhost");
        dc.setHoursOfLag(0);
        dc.setBrokerEndpoint("druid/v2");
        dc.setBrokerPort(1234);
        dc.setClusterName("name");
        when(dca.getDruidCluster(anyString())).thenReturn(dc);
        inject("clusterAccessor", dca);
        Response res = mock(Response.class);
        assertEquals(Routes.updateDruidCluster(req, res), Constants.SUCCESS);
        assertEquals(dc.getBrokerHost(), "hostname");
        assertEquals(dc.getBrokerPort(), (Integer) 431);
        verify(res, times(1)).status(200);
        verify(dca, times(1)).putDruidCluster(dc);
    }

    @Test
    public void testUpdateDruidClusterException() throws IOException, ClusterNotFoundException {
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
        inject("clusterAccessor", dca);
        when(dca.getDruidCluster(anyString())).thenThrow(new IOException("exception"));
        Response res = mock(Response.class);
        assertEquals(Routes.updateDruidCluster(fRequest, res), "exception");
        verify(res, times(1)).status(500);
    }

    @Test
    public void testGetDatabaseJsonDump() throws IOException {
        JsonObject o = new JsonObject();
        o.addProperty("key", "val");
        JsonDumper jd = mock(JsonDumper.class);
        when(jd.getRawData()).thenReturn(o);
        inject("jsonDumper", jd);
        assertEquals(Routes.getDatabaseJsonDump(fRequest, fResponse), "{\"key\":\"val\"}");
        when(jd.getRawData()).thenThrow(new IOException("exception"));
        assertEquals(Routes.getDatabaseJsonDump(fRequest, fResponse), "exception");
    }

    @Test
    public void testWriteDatabaseJsonDump() throws IOException {
        Request req = mock(Request.class);
        Response res = mock(Response.class);
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

    private Request req;
    private Response res;
    private DruidQueryService qs;
    private DetectorService ds;
    private DruidClusterAccessor dca;
    private JobMetadataAccessor jma;
    private AnomalyReportAccessor ara;
    private JobExecutionService jes;
    private ThymeleafTemplateEngine tte;

    private void mocks() {
        req = mock(Request.class);
        res = mock(Response.class);
        qs = mock(DruidQueryService.class);
        ds = mock(DetectorService.class);
        jes = mock(JobExecutionService.class);
        ServiceFactory sf = mock(ServiceFactory.class);
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
        @SuppressWarnings("unchecked") Map<String, Object> dp = (Map<String, Object>) mock(Map.class);
        inject("defaultParams", dp);
    }

    @Test
    public void testProcessInstantAnomalyJob() throws Exception {
        mocks();
        Query query = mock(Query.class);
        when(query.getQueryJsonObject()).thenReturn(new JsonObject());
        when(qs.build(anyString(), any(), anyInt(), anyInt())).thenReturn(query);
        QueryParamsMap map = mock(QueryParamsMap.class);
        Map<String, String[]> smap = new HashMap<>();
        when(map.toMap()).thenReturn(smap);
        when(req.queryMap()).thenReturn(map);
        smap.put("granularity", new String[]{"hour"});
        smap.put("clusterId", new String[]{"1"});
        smap.put("sigmaThreshold", new String[]{"3.5"});
        DruidClusterAccessor dca = mock(DruidClusterAccessor.class);
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
        ModelAndView mav = Routes.processInstantAnomalyJob(req, res);
        verify(tte, times(1)).render(any(ModelAndView.class));
        verify(jes, times(1)).getReports(any(), any());
        when(qs.build(any(), any(), anyInt(), anyInt())).thenThrow(new SherlockException());
        assertEquals(mav.getViewName(), "reportInstant");
        assertEquals(params(mav).get("tableHtml"), "<div></div>");
        mav = Routes.processInstantAnomalyJob(req, res);
        assertNotNull(params(mav).get(Constants.ERROR));
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
        verify(jes, times(2)).performBackfillJob(any(), any(), any(), anyInt(), anyInt(), any());
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
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        JobMetadata jm = new JobMetadata();
        Request req = mock(Request.class);
        when(req.params(Constants.ID)).thenReturn("1");
        Response res = mock(Response.class);
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
        JobMetadataAccessor jma = mock(JobMetadataAccessor.class);
        ServiceFactory sf = mock(ServiceFactory.class);
        JobExecutionService jes = mock(JobExecutionService.class);
        Request req = mock(Request.class);
        Response res = mock(Response.class);
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
        assertEquals(Routes.cloneJob(req, res), "rerun error");
        verify(res, times(1)).status(500);
    }
}
