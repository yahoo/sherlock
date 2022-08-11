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
import com.yahoo.sherlock.model.DetectorResult;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.DetectorConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.DBTestHelper;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyList;
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
    private static EgadsAPIService egadsAPIService;
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
        protected EgadsAPIService newEgadsAPIServiceInstance() {
            return egadsAPIService;
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
        druidQueryService = mockDruidQueryService;
        httpService = mockHttpService;
        timeSeriesParserService = mock(TimeSeriesParserService.class);
        egadsAPIService = mock(EgadsAPIService.class);
        when(mockDruidQueryService.build(anyString(), any(), Mockito.anyObject(), anyInt(), anyInt())).thenReturn(query);
        when(mockHttpService.queryDruidDatasources(Mockito.anyObject())).thenReturn(fakeDataSources);
        when(mockHttpService.queryDruid(Mockito.anyObject(), Mockito.anyObject())).thenReturn(jsonArray);
        when(timeSeriesParserService.parseTimeSeries(Mockito.anyObject(), Mockito.anyObject())).thenReturn(Collections.singletonList(timeseries));
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

    private static void inject(Class c, Object o, String name, Object v) {
        try {
            Field field = c.getDeclaredField(name);
            field.setAccessible(true);
            field.set(o, v);
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

    private DetectorService detectorService;
    private EgadsAPIService mockEgadsAPIService;
    private ProphetAPIService prophetAPIService;
    private TimeSeriesParserService mockTimeSeriesParserService;

    private void initMocks() {
        detectorService = mock(DetectorService.class);
        mockTimeSeriesParserService = mock(TimeSeriesParserService.class);
        mockEgadsAPIService = mock(EgadsAPIService.class);
        prophetAPIService = mock(ProphetAPIService.class);
        inject(detectorService, "parserService", mockTimeSeriesParserService);
        inject(detectorService, "egadsAPIService", mockEgadsAPIService);
        inject(detectorService, "prophetAPIService", prophetAPIService);
    }

    /**
     * Test the overloaded runDetection() calls the other main runDetection() method.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetection() throws SherlockException {
        initMocks();
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(detectorService.runDetection(any(JsonArray.class), any(), anyDouble(), any(DetectorConfig.class), anyString(), anyInt()))
                .thenCallRealMethod();
        Query query = mock(Query.class);
        when(query.getRunTime()).thenReturn(100000);
        when(query.getGranularity()).thenReturn(Granularity.HOUR);
        List<Anomaly> response = detectorService.runDetection(new JsonArray(), query, 0.0, mock(DetectorConfig.class), "day", 1);
        assertEquals(response.size(), 1);
    }

    /**
     * Test runDetection() when the input is null config and empty Time Series List.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetectionNoConfigEmptyTSList() throws Exception {
        initMocks();
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(mockEgadsAPIService.getNoDataAnomaly(any())).thenReturn(new Anomaly());
        when(detectorService.runDetection(any(), anyDouble(), any(), anyInt(), anyString(), any(Granularity.class), anyInt())).thenCallRealMethod();
        assertEquals(detectorService.runDetection(Collections.emptyList(), 0.0, null, 1234, null, null, 1).size(), 1);
        verify(mockEgadsAPIService, times(0)).preRunConfigure(any(), any(), any());
        verify(mockEgadsAPIService, times(0)).configureDetectionWindow(any(), any(), anyInt());
        verify(mockEgadsAPIService, times(0)).detectAnomalies(anyList(), anyInt());
    }

    /**
     * Test runDetection() when the input is Prophet forecast model and empty Time Series List.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetectionProphetConfigEmptyTsList() throws Exception {
        initMocks();
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(DetectorConfig.Framework.Prophet.toString());
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(detectorService.runDetection(any(), anyDouble(), any(), anyInt(), anyString(), any(Granularity.class), anyInt())).thenCallRealMethod();
        assertEquals(detectorService.runDetection(Collections.emptyList(), 0.0, config, 1234, null, null, 1).size(), 1);
        verify(prophetAPIService, times(1)).configureWith(any());
        verify(mockEgadsAPIService, times(0)).configureWith(any());
    }

    /**
     * Test runDetection() when the input is an arbitrary Egads forecast model and empty Time Series List.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetectionEgadsConfigEmptyTsList() throws Exception {
        initMocks();
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(DetectorConfig.Framework.Egads.toString());
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenReturn(Collections.singletonList(new Anomaly()));
        when(detectorService.runDetection(any(), anyDouble(), any(), anyInt(), anyString(), any(Granularity.class), anyInt())).thenCallRealMethod();
        List<String> tsModelList = DetectorConfig.TimeSeriesModel.getAllEgadsValues();
        for (int i = 0; i < tsModelList.size(); i++) {
            config.setTsModel(tsModelList.get(i));
            assertEquals(detectorService.runDetection(Collections.emptyList(), 0.0, config, 1234, null, null, 1).size(), 1);
        }
        verify(prophetAPIService, times(0)).configureWith(any());
        verify(mockEgadsAPIService, times(12)).configureWith(any());
    }

    /**
     * Test runDetection() when the input is null config and anomalies are detected.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetectionWithAnomaliesNullConfig() throws Exception {
        initMocks();
        List<Anomaly> anomalies = Lists.newArrayList(new Anomaly(), new Anomaly());
        List<TimeSeries> tslist = Lists.newArrayList(new TimeSeries(), new TimeSeries());
        when(mockEgadsAPIService.detectAnomalies(anyList(), anyInt())).thenReturn(anomalies);
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenCallRealMethod();
        // run with null config
        List<Anomaly> result = detectorService.runDetection(tslist, 3.0, null, 123, "day", Granularity.DAY, 1);
        assertEquals(result.size(), 2);
        verify(mockEgadsAPIService, times(0)).configureWith(any());
        verify(mockEgadsAPIService, times(1)).preRunConfigure(any(), any(), anyInt());
        verify(mockEgadsAPIService, times(1)).configureDetectionWindow(any(), any(), anyInt());
        verify(mockEgadsAPIService, times(1)).detectAnomalies(anyList(), anyInt());
        verify(prophetAPIService, times(0)).configureWith(any());
        verify(prophetAPIService, times(0)).preRunConfigure(any(), any(), anyInt());
        verify(prophetAPIService, times(0)).configureDetectionWindow(any(), any(), anyInt());
        verify(prophetAPIService, times(0)).detectAnomalies(anyList(), anyInt());
    }

    /**
     * Test runDetection() runs the Egads route when passing in Egads as the argument;
     * tested with all available Egads Time Series Forecasting models.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetectionWithAnomaliesEgadsConfig() throws Exception {
        initMocks();
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(DetectorConfig.Framework.Egads.toString());
        List<Anomaly> anomalies = Lists.newArrayList(new Anomaly(), new Anomaly(), new Anomaly());
        List<TimeSeries> tslist = Lists.newArrayList(new TimeSeries(), new TimeSeries(), new TimeSeries());
        when(mockEgadsAPIService.detectAnomalies(anyList(), anyInt())).thenReturn(anomalies);
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenCallRealMethod();
        // run with Egads config
        List<String> tsModelList = DetectorConfig.TimeSeriesModel.getAllEgadsValues();
        for (int i = 0; i < tsModelList.size(); i++) {
            config.setTsModel(tsModelList.get(i));
            List<Anomaly> result = detectorService.runDetection(tslist, 3.0, config, 123, "day", Granularity.DAY, 1);
            assertEquals(result.size(), 3);
            verify(mockEgadsAPIService, times(i + 1)).configureWith(any());
            verify(mockEgadsAPIService, times(i + 1)).preRunConfigure(any(), any(), anyInt());
            verify(mockEgadsAPIService, times(i + 1)).configureDetectionWindow(any(), any(), anyInt());
            verify(mockEgadsAPIService, times(i + 1)).detectAnomalies(anyList(), anyInt());
            verify(prophetAPIService, times(0)).configureWith(any());
            verify(prophetAPIService, times(0)).preRunConfigure(any(), any(), anyInt());
            verify(prophetAPIService, times(0)).configureDetectionWindow(any(), any(), anyInt());
            verify(prophetAPIService, times(0)).detectAnomalies(anyList(), anyInt());
        }
    }

    /**
     * Test runDetection() runs the Prophet route when passing in Prophet as the argument.
     * @throws Exception Exception
     */
    @Test
    public void testRunDetectionWithAnomaliesProphetConfig() throws Exception {
        initMocks();
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(DetectorConfig.Framework.Prophet.toString());
        List<Anomaly> anomalies = Lists.newArrayList(new Anomaly(), new Anomaly());
        List<TimeSeries> tslist = Lists.newArrayList(new TimeSeries(), new TimeSeries());
        when(mockEgadsAPIService.detectAnomalies(anyList(), anyInt())).thenReturn(anomalies);
        when(prophetAPIService.detectAnomalies(anyList(), anyInt())).thenReturn(Lists.newArrayList(new Anomaly(), new Anomaly(), new Anomaly()));
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenCallRealMethod();
        // Run with Prophet
        config.setTsModel(Constants.PROPHET);
        List<Anomaly> result = detectorService.runDetection(tslist, 3.0, config, 123, "day", Granularity.DAY, 1);
        assertEquals(result.size(), 3);
        verify(mockEgadsAPIService, times(0)).configureWith(any());
        verify(mockEgadsAPIService, times(0)).preRunConfigure(any(), any(), anyInt());
        verify(mockEgadsAPIService, times(0)).configureDetectionWindow(any(), any(), anyInt());
        verify(mockEgadsAPIService, times(0)).detectAnomalies(anyList(), anyInt());
        verify(prophetAPIService, times(1)).configureWith(any());
        verify(prophetAPIService, times(1)).preRunConfigure(any(), any(), anyInt());
        verify(prophetAPIService, times(1)).configureDetectionWindow(any(), any(), anyInt());
        verify(prophetAPIService, times(1)).detectAnomalies(anyList(), anyInt());
    }

    /**
     * Unit tests runDetection() throws Exception when passing Illegal Forecasting Framework as argument.
     */
    @Test
    public void testRunDetectionFrameworkException() throws Exception {
        initMocks();
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        // use an invalid TimeSeries framework name
        config.setTsFramework("Invalid TimeSeries Framework");
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenCallRealMethod();
        try {
            detectorService.runDetection(tslist, 3.0, config, 123, "day", Granularity.DAY, 1);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Time Series Framework not identified.");
            return;
        }
        Assert.fail();
    }

    /**
     * Unit tests runDetection() throws Exception when passing Illegal Egads TimeSeries Model as argument.
     */
    @Test
    public void testRunDetectionTSModelException() throws Exception {
        initMocks();
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(DetectorConfig.Framework.Egads.toString());
        // use an invalid Egads TimeSeries Model name
        config.setTsModel("Invalid Egads TimeSeries Model");
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(detectorService.runDetection(any(), anyDouble(), any(DetectorConfig.class), anyInt(), anyString(), any(Granularity.class), anyInt()))
                .thenCallRealMethod();
        try {
            detectorService.runDetection(tslist, 3.0, config, 123, "day", Granularity.DAY, 1);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Egads Time Series Forecasting Model not identified.");
            return;
        }
        Assert.fail();
    }

    /**
     * Test method detectWithResults() runs the Egads route when passing
     * in Egads TimeSeries Models.
     */
    @Test
    public void testDetectWithResults() throws Exception {
        DetectorResult res = new DetectorResult();
        initMocks();
        when(mockEgadsAPIService.detectAnomaliesAndForecast(any(List.class))).thenCallRealMethod();
        when(mockEgadsAPIService.detectAnomaliesAndForecast(any(TimeSeries.class))).thenReturn(res);
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(mockTimeSeriesParserService.parseTimeSeries(any(), any())).thenReturn(tslist);
        when(detectorService.detectWithResults(any(), any(), any(), any(), any())).thenCallRealMethod();
        Query query = new Query(null, 150000000, 159999999, Granularity.DAY, 1);
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(Constants.EGADS);
        List<String> tsModelList = DetectorConfig.TimeSeriesModel.getAllEgadsValues();
        for (int i = 0; i < tsModelList.size(); i++) {
            config.setTsModel(tsModelList.get(i));
            List<DetectorResult> reslist = detectorService.detectWithResults(query, 3.0, new DruidCluster(), 1, config);
            assertEquals(5, reslist.size());
            verify(mockEgadsAPIService, times(i + 1)).configureWith(any());
            verify(mockEgadsAPIService, times(i + 1)).preRunConfigure(any(), any(), anyInt());
            verify(mockEgadsAPIService, times(i + 1)).configureDetectionWindow(query.getRunTime() / 60, query.getGranularity().toString(), 2);
            verify(mockEgadsAPIService, times(5 * (i + 1))).detectAnomaliesAndForecast(any(TimeSeries.class));
        }
    }

    /**
     * Test method detectWithResults() runs the Prophet route when
     * passing in Prophet as TimeSeries Model.
     */
    @Test
    public void testProphetDetectWithResults() throws Exception {
        List<DetectorResult> res = Lists.newArrayList(new DetectorResult(), new DetectorResult(),
                new DetectorResult(), new DetectorResult(), new DetectorResult());
        initMocks();
        when(prophetAPIService.detectAnomaliesAndForecast(anyList())).thenReturn(res);
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(mockTimeSeriesParserService.parseTimeSeries(any(), any())).thenReturn(tslist);
        when(detectorService.detectWithResults(any(), any(), any(), any(), any())).thenCallRealMethod();
        Query query = new Query(null, 150000000, 159999999, Granularity.DAY, 1);
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(Constants.PROPHET);
        config.setTsModel(Constants.PROPHET);
        List<DetectorResult> reslist = detectorService.detectWithResults(query, 3.0, new DruidCluster(), 1, config);
        assertEquals(5, reslist.size());
        verify(prophetAPIService, times(1)).configureWith(any());
        verify(prophetAPIService, times(1)).preRunConfigure(any(), any(), anyInt());
        verify(prophetAPIService, times(1)).configureDetectionWindow(query.getRunTime() / 60, query.getGranularity().toString(), 2);
        verify(prophetAPIService, times(1)).detectAnomaliesAndForecast(anyList());
    }

    /**
     * Test method detectWithResults() throws Exception when passing
     * Illegal TimeSeries Framework as argument.
     */
    @Test
    public void testDetectWithResultsFrameworkException() throws Exception {
        List<DetectorResult> res = new ArrayList<>();
        initMocks();
        when(prophetAPIService.detectAnomaliesAndForecast(anyList())).thenReturn(res);
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(mockTimeSeriesParserService.parseTimeSeries(any(), any())).thenReturn(tslist);
        when(detectorService.detectWithResults(any(), any(), any(), any(), any())).thenCallRealMethod();
        Query query = new Query(null, 150000000, 159999999, Granularity.DAY, 1);
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        // use an invalid forecasting framework name
        config.setTsFramework("Invalid TimeSeries Framework");
        try {
            detectorService.detectWithResults(query, 3.0, new DruidCluster(), 1, config);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Time Series Framework not identified.");
            return;
        }
        Assert.fail();
    }

    /**
     * Test method detectWithResults() throws Exception when passing
     * Illegal Egads Time Series Forecasting Model as argument.
     */
    @Test
    public void testDetectWithResultsEgadsModelException() throws Exception {
        List<DetectorResult> res = new ArrayList<>();
        initMocks();
        when(prophetAPIService.detectAnomaliesAndForecast(anyList())).thenReturn(res);
        List<TimeSeries> tslist = Lists.newArrayList(
                new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()
        );
        when(mockTimeSeriesParserService.parseTimeSeries(any(), any())).thenReturn(tslist);
        when(detectorService.detectWithResults(any(), any(), any(), any(), any())).thenCallRealMethod();
        Query query = new Query(null, 150000000, 159999999, Granularity.DAY, 1);
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsFramework(Constants.EGADS);
        // use an invalid Egads Time Series Model name
        config.setTsModel("Invalid Egads TimeSeries Model");
        try {
            detectorService.detectWithResults(query, 3.0, new DruidCluster(), 1, config);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Egads Time Series Forecasting Model not identified.");
            return;
        }
        Assert.fail();
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
        } catch (DruidException e) {
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
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "Querying unknown datasource: [s1, s2]");
        }
    }

}
