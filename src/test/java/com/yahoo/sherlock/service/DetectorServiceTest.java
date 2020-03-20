/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.beust.jcommander.internal.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.store.DBTestHelper;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Test detector service.
 */
public class DetectorServiceTest {

    private static DruidQueryService druidQueryService;
    private static HttpService httpService;
    private static TimeSeriesParserService timeSeriesParserService;
    private static EgadsService egadsService;
    private static String queryString;
    private static JsonArray fakeDataSources;
    private Gson gson = new Gson();

    private static class MockServiceFactory extends ServiceFactory {
        @Override
        public DruidQueryService newDruidQueryServiceInstance() {
            return druidQueryService;
        }

        @Override
        protected HttpService newHttpServiceInstance() {
            return httpService;
        }

        @Override
        public TimeSeriesParserService newTimeSeriesParserServiceInstance() {
            return timeSeriesParserService;
        }

        @Override
        protected EgadsService newEgadsServiceInstance() {
            return egadsService;
        }
    }

    private static class MockDetectorService extends DetectorService {
    }

    @BeforeMethod
    public void setUp() throws Exception {
        queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_1.json")));
        JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        Query query = new Query(queryJsonObject, 1, 1234, Granularity.HOUR, 1);
        String druidResponse = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_valid_response_1.json")));
        JsonArray jsonArray = gson.fromJson(druidResponse, JsonArray.class);
        TimeSeries timeseries = new TimeSeries();
        timeseries.meta.id = "123zx23";
        timeseries.meta.name = "metric";
        timeseries.meta.source = "filter1, filter2";
        timeseries.append(3600L, 11.11f);
        timeseries.append(7200L, 22.22f);
        timeseries.append(10800L, 33.33f);
        timeseries.append(14400L, 55.55f);
        timeseries.append(18000L, 33.33f);
        timeseries.append(21600L, 77.77f);
        timeseries.append(25200L, 99.99f);
        timeseries.append(36000L, 100.99f);
        List<Anomaly> anomalies = new ArrayList<>();
        long start = 1508348470;
        long end = 1508349400;
        Anomaly anomaly = new Anomaly();
        anomaly.metricMetaData.id = "123zx23";
        anomaly.metricMetaData.name = "metric";
        anomaly.metricMetaData.source = "filter1, filter2";
        anomaly.addInterval(start, end, 0.5F);
        anomalies.add(anomaly);
        ServiceFactory mockServiceFactory = new MockServiceFactory();
        fakeDataSources = gson.fromJson("[\"datastore\"]", JsonArray.class);
        DruidQueryService mockDruidQueryService = mock(DruidQueryService.class);
        HttpService mockHttpService = mock(HttpService.class);
        TimeSeriesParserService mockTimeSeriesParserService = mock(TimeSeriesParserService.class);
        EgadsService mockEgadsService = mock(EgadsService.class);
        when(mockDruidQueryService.build(anyString(), any(), Mockito.anyObject(), anyInt(), anyInt())).thenReturn(query);
        when(mockHttpService.queryDruidDatasources(Mockito.anyObject())).thenReturn(fakeDataSources);
        when(mockHttpService.queryDruid(Mockito.anyObject(), Mockito.anyObject())).thenReturn(jsonArray);
        when(mockTimeSeriesParserService.parseTimeSeries(Mockito.anyObject(), Mockito.anyObject())).thenReturn(Collections.singletonList(timeseries));
        when(mockEgadsService.runEGADS(Mockito.anyObject(), anyDouble())).thenReturn(anomalies);
        druidQueryService = mockDruidQueryService;
        httpService = mockHttpService;
        timeSeriesParserService = mockTimeSeriesParserService;
        egadsService = mockEgadsService;
    }

    private static void inject(Object o, String name, Object v) {
        try {
            Field f = DetectorService.class.getDeclaredField(name);
            f.setAccessible(true);
            f.set(o, v);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void testExceptions() throws Exception {
        JobMetadata job = new JobMetadata();
        job.setJobId(1);
        job.setQuery(queryString);
        job.setGranularity("day");
        job.setSigmaThreshold(3.0);
        fakeDataSources = gson.fromJson("[\"datastore\"]", JsonArray.class);
        HttpService mockHttpService = mock(HttpService.class);
        when(mockHttpService.queryDruidDatasources(Mockito.anyObject())).thenReturn(fakeDataSources);
        httpService = mockHttpService;
        DetectorService detectorService = new MockDetectorService();
        inject(detectorService, "httpService", httpService);
        try {
            detectorService.detect(DBTestHelper.getNewDruidCluster(), job);
            Assert.fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Querying unknown datasource: [s1]");
        }
    }

    private DetectorService ds;
    private EgadsService egads;
    private TimeSeriesParserService ps;

    private void initMocks() {
        ds = mock(DetectorService.class);

        ps = mock(TimeSeriesParserService.class);
        egads = mock(EgadsService.class);
        inject(ds, "parserService", ps);
        inject(ds, "egads", egads);
    }

    @Test
    public void testRunDetection() throws SherlockException {
        initMocks();
        when(ds.runDetection(any(), anyDouble(), any(EgadsConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(ds.runDetection(any(JsonArray.class), any(), anyDouble(), any(EgadsConfig.class), anyString(), anyInt()))
                .thenCallRealMethod();
        Query query = mock(Query.class);
        when(query.getRunTime()).thenReturn(100000);
        when(query.getGranularity()).thenReturn(Granularity.HOUR);
        List<Anomaly> response = ds.runDetection(new JsonArray(), query, 0.0, mock(EgadsConfig.class), "day", 1);
        assertEquals(response.size(), 1);
    }

    @Test
    public void testRunDetectionNoConfig() throws Exception {
        initMocks();
        when(ds.runDetection(any(), anyDouble(), any(EgadsConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(ds.runDetection(any(), anyDouble(), any(), anyInt(), anyString(), any(Granularity.class), anyInt())).thenCallRealMethod();
        assertEquals(ds.runDetection(Collections.emptyList(), 0.0, null, 1234, null, null, 1).size(), 1);
    }

    @Test
    public void testRunDetectionWithConfig() throws Exception {
        initMocks();
        List<Anomaly> anomalies = Lists.newArrayList(new Anomaly(), new Anomaly());
        TimeSeries endSeries = new TimeSeries();
        endSeries.data = new TimeSeries.DataSequence();
        endSeries.data.add(new TimeSeries.Entry(123 * 60, 1000));
        List<TimeSeries> tslist = Lists.newArrayList(endSeries, new TimeSeries());
        Properties p = new Properties();
        p.setProperty("AD_MODEL", "model1");
        when(egads.getP()).thenReturn(p);
        when(egads.runEGADS(any(), anyDouble())).thenReturn(anomalies);
        when(ds.runDetection(any(), anyDouble(), any(EgadsConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenCallRealMethod();
        List<Anomaly> result = ds.runDetection(tslist, 3.0, null, 123, "day", Granularity.DAY, 1);
        assertEquals(result.size(), 3);
        result = ds.runDetection(tslist, 3.0, mock(EgadsConfig.class), 123, "day", Granularity.DAY, 1);
        assertEquals(result.size(), 3);
        verify(egads, times(2)).runEGADS(any(), any());
        verify(egads, times(1)).preRunConfigure(any(), any(), anyInt());
        verify(egads, times(1)).configureWith(any());
    }

    @Test
    public void testDetectWithResults() throws Exception {
        EgadsResult res = new EgadsResult();
        initMocks();
        when(egads.detectAnomaliesResult(any())).thenReturn(res);
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(ps.parseTimeSeries(any(), any())).thenReturn(tslist);
        when(ds.detectWithResults(any(), any(), any(), any(), any())).thenCallRealMethod();
        Query query = new Query(null, 150000000, 159999999, Granularity.DAY, 1);
        List<EgadsResult> reslist = ds.detectWithResults(query, 3.0, new DruidCluster(), 1, new EgadsConfig());
        assertEquals(5, reslist.size());
        verify(egads, times(1)).configureWith(any());
        verify(egads, times(1)).preRunConfigure(any(), any(), anyInt());
        verify(egads, times(1)).configureDetectionWindow(query.getRunTime() / 60, query.getGranularity().toString(), 2);
        verify(egads, times(5)).detectAnomaliesResult(any());
    }

    @Test
    public void testCheckDataSourceWithValidUnionOfDataSourceQuery() throws Exception {
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_4.json")));
        JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        Query query = new Query(queryJsonObject, 1, 1234, Granularity.HOUR, 1);
        DruidCluster dc = new DruidCluster();

        HttpService mockHttpService = mock(HttpService.class);
        JsonArray fakeDataSources = gson.fromJson("[\"s1\", \"s2\", \"s3\"]", JsonArray.class);
        when(mockHttpService.queryDruidDatasources(Mockito.anyObject())).thenReturn(fakeDataSources);
        httpService = mockHttpService;
        try {
            DetectorService detectorService = new MockDetectorService();
            inject(detectorService, "httpService", httpService);
            detectorService.checkDatasource(query, dc);
        } catch (DruidException e){
            Assert.fail();
        }
    }

    @Test
    public void testCheckDataSourceWithInValidUnionOfDataSourceQuery() throws Exception {
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_4.json")));
        JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        Query query = new Query(queryJsonObject, 1, 1234, Granularity.HOUR, 1);
        DruidCluster dc = new DruidCluster();
        HttpService mockHttpService = mock(HttpService.class);
        JsonArray fakeDataSources = gson.fromJson("[\"s3\", \"s4\"]", JsonArray.class);
        when(mockHttpService.queryDruidDatasources(Mockito.anyObject())).thenReturn(fakeDataSources);
        try {
            DetectorService detectorService = new MockDetectorService();
            inject(detectorService, "httpService", mockHttpService);
            detectorService.checkDatasource(query, dc);
            Assert.fail();
        } catch (DruidException e){
            assertEquals(e.getMessage(), "Querying unknown datasource: [s1, s2]");
        }
    }

}
