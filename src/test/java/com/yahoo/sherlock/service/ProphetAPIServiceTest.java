/*
 * Copyright 2022, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.yahoo.egads.control.AnomalyDetector;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.Anomaly.IntervalSequence;
import com.yahoo.egads.data.Anomaly.Interval;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.models.adm.AnomalyDetectionModel;
import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.exception.DetectorServiceException;
import com.yahoo.sherlock.model.DetectorResult;
import com.yahoo.sherlock.query.DetectorConfig;

import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Test for Prophet service.
 */
public class ProphetAPIServiceTest {

    /**
     * Class Prophet API service instance.
     */
    ProphetAPIService prophetAPIService;
    ProphetAPIService mockProphetAPIService;
    HttpService httpService;
    Properties p;
    JsonObject tsObject1;
    JsonObject tsObject2;
    JsonObject tsObject3;
    JsonObject forecastedObject;

    List<TimeSeries> tsList;

    /**
     * Mock() method to set up all mock variables.
     */
    private void mocks() {
        httpService = mock(HttpService.class);
        prophetAPIService = mock(ProphetAPIService.class);
        mockProphetAPIService = new MockProphetAPIService();
    }

    private static class MockProphetAPIService extends ProphetAPIService {

    }

    /**
     * Set up other non-mock variables.
     */
    @BeforeMethod
    public void setUp() throws Exception {
        p = DetectorConfig.create().buildDefault().asProperties();
        tsObject1 = new JsonObject();
        tsObject1.addProperty("60", 11.11f);
        tsObject1.addProperty("120", 22.22f);
        tsObject1.addProperty("180", 33.33f);

        tsObject2 = new JsonObject();
        tsObject2.addProperty("240", 44.44f);
        tsObject2.addProperty("300", 55.55f);
        tsObject2.addProperty("360", 66.66f);

        tsObject3 = new JsonObject();
        tsObject3.addProperty("420", 77.77f);
        tsObject3.addProperty("480", 88.88f);
        tsObject3.addProperty("540", 99.99f);

        forecastedObject = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(tsObject1);
        jsonArray.add(tsObject2);
        jsonArray.add(tsObject3);
        forecastedObject.add("forecasted", jsonArray);

        TimeSeries series1 = new TimeSeries(new long[]{60L, 120L, 180L}, new float[]{11.11f, 22.22f, 33.33f});
        series1.meta.id = "123zx23";
        series1.meta.name = "metric";
        series1.meta.source = "filter1, filter2";
        TimeSeries series2 = new TimeSeries(new long[]{240L, 300L, 360L}, new float[]{44.44f, 55.55f, 66.66f});
        series2.meta.id = "123zx23";
        series2.meta.name = "metric";
        series2.meta.source = "filter1, filter2";
        TimeSeries series3 = new TimeSeries(new long[]{420L, 480L, 540L}, new float[]{77.77f, 88.88f, 99.99f});
        series3.meta.id = "123zx23";
        series3.meta.name = "metric";
        series3.meta.source = "filter1, filter2";
        tsList = new ArrayList<>(Arrays.asList(series1, series2, series3));
    }

    /**
     * Helper method to inject field variable v into class instance o.
     * @param c class of instance o
     * @param o Class instance o
     * @param name field to be modified
     * @param v field variable being injected
     */
    private static void inject(Class c, Object o, String name, Object v) {
        try {
            Field field = c.getDeclaredField(name);
            field.setAccessible(true);
            field.set(o, v);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

    /**
     * Test listParamsToJson() method that converts Prophet parameters to a Json query.
     * @throws Exception Exception
     */
    @Test
    public void testListParamsToJson() {
        mocks();
        when(prophetAPIService.listParamsToJson(any(), any(), any(), any(), any(List.class))).thenCallRealMethod();
        JsonObject actual = prophetAPIService.listParamsToJson("linear", "False", "True", "auto", tsList);
        JsonObject expected = new JsonObject();
        expected.addProperty("growth", "linear");
        expected.addProperty("yearly_seasonality", "False");
        expected.addProperty("weekly_seasonality", "True");
        expected.addProperty("daily_seasonality", "auto");
        JsonArray tsArray = new JsonArray();
        tsArray.add(tsObject1);
        tsArray.add(tsObject2);
        tsArray.add(tsObject3);
        expected.add("timeseries", tsArray);
        assertTrue(actual.equals(expected));
    }

    /**
     * Test jsonToDataSequenceList() method that converts Json response to a list of DataSequence.
     * @throws Exception Exception
     */
    @Test void testJsonToDataSequenceList() {
        mocks();
        when(prophetAPIService.jsonToDataSequenceList(any(JsonObject.class))).thenCallRealMethod();
        // Compute the actual DataSequence list
        List<TimeSeries.DataSequence> actualList = prophetAPIService.jsonToDataSequenceList(forecastedObject);
        // Generate the expected DataSequence list
        List<TimeSeries.DataSequence> expectedList = new ArrayList<>(
                Arrays.asList(tsList.get(0).data, tsList.get(1).data, tsList.get(2).data)
        );
        Assert.assertEquals(actualList, expectedList);
    }

    /**
     * Test method detectAnomaliesAndForecast() handles Exception when it queries the Prophet Microservice.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomaliesResultException() throws Exception {
        mocks();
        inject(ProphetAPIService.class, mockProphetAPIService, "httpService", httpService);
        inject(ProphetAPIService.class.getSuperclass(), mockProphetAPIService, "p", p);
        when(httpService.queryProphetService(any(), any())).thenThrow(new DetectorServiceException("Prophet Rest endpoint failed with HTTP Status 500"));
        try {
            mockProphetAPIService.detectAnomaliesAndForecast(tsList);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Prophet Rest endpoint failed with HTTP Status 500");
            return;
        }
        Assert.fail();
    }

    /**
     * Test method detectAnomaliesAndForecast() uses the specified anomaly detection model.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomaliesResult() throws Exception {
        // Set up
        ArgumentCaptor<AnomalyDetector> adCaptor = ArgumentCaptor.forClass(AnomalyDetector.class);
        HttpService hService = mock(HttpService.class);
        when(hService.queryProphetService(any(), any())).thenReturn(forecastedObject);
        // Test against all Egads Anomaly Detection models
        for (String name : DetectorConfig.AnomalyDetectionModel.getAllValues()) {
            Properties prop = DetectorConfig.create().buildDefault().asProperties();
            prop.setProperty(DetectorConfig.AD_MODEL, name);
            ProphetAPIService prophetAPIServiceSpy = spy(new ProphetAPIService(hService, prop));
            try {
                prophetAPIServiceSpy.configureThreshold(); // configure threshold in advance to avoid NaiveModel failure
                prophetAPIServiceSpy.detectAnomaliesAndForecast(tsList);
            } catch (Exception e) {
                Assert.fail();
            }
            verify(prophetAPIServiceSpy, times(3)).getAnomalies(adCaptor.capture(), any(), any());
            AnomalyDetector ad = adCaptor.getValue();
            ArrayList<AnomalyDetectionModel> adModels = (ArrayList<AnomalyDetectionModel>) TestUtilities.obtain(ad, ad.getClass(), "models");
            Assert.assertEquals(adModels.get(0).getModelName(), name);
        }
    }

    /**
     * Test method detectAnomaliesAndForecast() when there is no anomaly.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomaliesNoAnomaly() throws Exception {
        mocks();
        inject(ProphetAPIService.class, mockProphetAPIService, "httpService", httpService);
        inject(ProphetAPIService.class.getSuperclass(), mockProphetAPIService, "p", p);
        when(httpService.queryProphetService(any(), any())).thenReturn(forecastedObject);
        List<Anomaly> anomalies = new ArrayList<>();
        Anomaly anomaly = new Anomaly();
        anomaly.id = "metric";
        anomaly.type = "point_outlier";
        anomaly.modelName = "KSigmaModel";
        anomaly.metricMetaData.id = "123zx23";
        anomaly.metricMetaData.name = "metric";
        anomaly.metricMetaData.source = "filter1, filter2";
        anomalies.add(anomaly);
        DetectorResult res1 = new DetectorResult(anomalies, tsList.get(0), tsList.get(0).data);
        DetectorResult res2 = new DetectorResult(anomalies, tsList.get(1), tsList.get(1).data);
        DetectorResult res3 = new DetectorResult(anomalies, tsList.get(2), tsList.get(2).data);
        List<DetectorResult> expectedResult = new ArrayList<>(
                Arrays.asList(res1, res2, res3)
        );
        List<DetectorResult> actualResult = null;
        try {
            actualResult = mockProphetAPIService.detectAnomaliesAndForecast(tsList);
        } catch (Exception e) {
            Assert.fail();
        }
        assertEquals(actualResult, expectedResult);
    }

    /**
     * Test detectAnomaliesAndForecast() when it detects anomalies.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomaliesResultHasAnomaly() throws Exception {
        mocks();
        inject(ProphetAPIService.class, mockProphetAPIService, "httpService", httpService);
        inject(ProphetAPIService.class.getSuperclass(), mockProphetAPIService, "p", p);
        TimeSeries series1 = new TimeSeries(new long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L},
                new float[]{11.11f, 11.11f, 11.11f, 9999999.99f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f});
        series1.meta.id = "123zx23";
        series1.meta.name = "metric";
        series1.meta.source = "filter1, filter2";

        TimeSeries series2 = new TimeSeries(new long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L},
                new float[]{11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f});
        series2.meta.id = "123zx23";
        series2.meta.name = "metric";
        series2.meta.source = "filter1, filter2";
        series2.data.setLogicalIndices(1, 1);

        // Initialize Prophet Service response object
        JsonObject obj = new JsonObject();
        obj.addProperty("1", 11.11f);
        obj.addProperty("2", 11.11f);
        obj.addProperty("3", 11.11f);
        obj.addProperty("4", 11.11f);
        obj.addProperty("5", 11.11f);
        obj.addProperty("6", 11.11f);
        obj.addProperty("7", 11.11f);
        obj.addProperty("8", 11.11f);
        obj.addProperty("9", 11.11f);
        obj.addProperty("10", 11.11f);
        obj.addProperty("11", 11.11f);
        obj.addProperty("12", 11.11f);
        obj.addProperty("13", 11.11f);
        obj.addProperty("14", 11.11f);
        obj.addProperty("15", 11.11f);
        JsonObject responseObj = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(obj);
        jsonArray.add(obj);
        responseObj.add("forecasted", jsonArray);
        when(httpService.queryProphetService(any(), any())).thenReturn(responseObj);

        // Initialize Anomaly points
        List<Anomaly> anomalies1 = new ArrayList<>();
        List<Anomaly> anomalies2 = new ArrayList<>();
        Anomaly anomaly1 = new Anomaly();
        anomaly1.id = "metric";
        anomaly1.type = "point_outlier";
        anomaly1.modelName = "KSigmaModel";
        anomaly1.metricMetaData.id = "123zx23";
        anomaly1.metricMetaData.name = "metric";
        anomaly1.metricMetaData.source = "filter1, filter2";
        Float[] errors = new Float[]{9.0008904E7f, 9999989.0f, 199.99956f, 99.99989f, 7.0f};
        Float[] threshold = new Float[]{7.335708E7f, 8149973.0f, 162.99928f, 81.499725f, 5.7049866f};
        IntervalSequence is = new IntervalSequence();
        is.add(new Interval(4L, 3, errors, threshold, 9999999.99f, 11.11f));
        anomaly1.intervals = is;
        anomaly1.intervals.setLogicalIndices(1L, 1);
        anomaly1.intervals.setTimeStamps(1L, 1);
        anomalies1.add(anomaly1);
        Anomaly anomaly2 = new Anomaly();
        anomaly2.id = "metric";
        anomaly2.type = "point_outlier";
        anomaly2.modelName = "KSigmaModel";
        anomaly2.metricMetaData.id = "123zx23";
        anomaly2.metricMetaData.name = "metric";
        anomaly2.metricMetaData.source = "filter1, filter2";
        anomalies2.add(anomaly2);

        // Set up actual input/result + expected result
        List<TimeSeries> actualInput = new ArrayList<>(
                Arrays.asList(series1, series2)
        );
        List<DetectorResult> actualResult = null;
        DetectorResult res1 = new DetectorResult(anomalies1, series1, series2.data);
        DetectorResult res2 = new DetectorResult(anomalies2, series2, series2.data);
        List<DetectorResult> expectedResult = new ArrayList<>(
                Arrays.asList(res1, res2)
        );

        // Compute actual result
        try {
            actualResult = mockProphetAPIService.detectAnomaliesAndForecast(actualInput);
        } catch (Exception e) {
            Assert.fail();
        }
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Test method detectAnomalies() handles Exception when it queries the Prophet Service.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomaliesException() throws Exception {
        mocks();
        inject(ProphetAPIService.class, mockProphetAPIService, "httpService", httpService);
        inject(ProphetAPIService.class.getSuperclass(), mockProphetAPIService, "p", p);
        when(httpService.queryProphetService(any(), any())).thenThrow(new DetectorServiceException("Prophet Rest endpoint failed with HTTP Status 500"));
        try {
            mockProphetAPIService.detectAnomalies(tsList, 3);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Prophet Rest endpoint failed with HTTP Status 500");
            return;
        }
        Assert.fail();
    }

    /**
     * Test method detectAnomalies() uses the specified anomaly detection model.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomalies() throws Exception {
        // Set up
        ArgumentCaptor<AnomalyDetector> adCaptor = ArgumentCaptor.forClass(AnomalyDetector.class);
        HttpService hService = mock(HttpService.class);
        when(hService.queryProphetService(any(), any())).thenReturn(forecastedObject);
        // Test against all Egads Anomaly Detection models
        for (String name : DetectorConfig.AnomalyDetectionModel.getAllValues()) {
            Properties prop = DetectorConfig.create().buildDefault().asProperties();
            prop.setProperty(DetectorConfig.AD_MODEL, name);
            ProphetAPIService prophetAPIServiceSpy = spy(new ProphetAPIService(hService, prop));
            List<Anomaly> anomalies = new ArrayList<>();
            try {
                prophetAPIServiceSpy.configureThreshold(); // configure threshold in advance to avoid NaiveModel failure
                anomalies = prophetAPIServiceSpy.detectAnomalies(tsList, 3);
            } catch (Exception e) {
                Assert.fail();
            }
            assertEquals(anomalies.size(), 3);
            // Only the 1st time series is used for detection
            verify(prophetAPIServiceSpy, times(1)).getAnomalies(adCaptor.capture(), any(), any());
            AnomalyDetector ad = adCaptor.getValue();
            ArrayList<AnomalyDetectionModel> adModels = (ArrayList<AnomalyDetectionModel>) TestUtilities.obtain(ad, ad.getClass(), "models");
            Assert.assertEquals(adModels.get(0).getModelName(), name);
        }
    }

    /**
     * Test detectAnomalies() generates a list of anomalies.
     * @throws Exception Exception
     */
    @Test
    public void testDetectAnomaliesHasAnomaly() throws Exception {
        mocks();
        inject(ProphetAPIService.class, mockProphetAPIService, "httpService", httpService);
        inject(ProphetAPIService.class.getSuperclass(), mockProphetAPIService, "p", p);
        TimeSeries series1 = new TimeSeries(new long[]{0L, 60L, 120L, 180L, 240L, 300L, 360L, 420L, 480L, 540L, 600L, 660L, 720L, 780L, 840L},
                new float[]{11.11f, 11.11f, 11.11f, 11.11f, 9999999.99f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f});
        series1.meta.id = "123zx23";
        series1.meta.name = "metric";
        series1.meta.source = "filter1, filter2";

        TimeSeries series2 = new TimeSeries(new long[]{0L, 60L, 120L, 180L, 240L, 300L, 360L, 420L, 480L, 540L, 600L, 660L, 720L, 780L, 840L},
                new float[]{11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f, 11.11f});
        series2.meta.id = "123zx23";
        series2.meta.name = "metric";
        series2.meta.source = "filter1, filter2";
        series2.data.setLogicalIndices(1, 1);

        // Initialize Prophet Service response
        JsonObject obj = new JsonObject();
        obj.addProperty("0", 11.11f);
        obj.addProperty("60", 11.11f);
        obj.addProperty("120", 11.11f);
        obj.addProperty("180", 11.11f);
        obj.addProperty("240", 11.11f);
        obj.addProperty("300", 11.11f);
        obj.addProperty("360", 11.11f);
        obj.addProperty("420", 11.11f);
        obj.addProperty("480", 11.11f);
        obj.addProperty("540", 11.11f);
        obj.addProperty("600", 11.11f);
        obj.addProperty("660", 11.11f);
        obj.addProperty("720", 11.11f);
        obj.addProperty("780", 11.11f);
        obj.addProperty("840", 11.11f);
        JsonObject responseObj = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(obj);
        jsonArray.add(obj);
        responseObj.add("forecasted", jsonArray);
        when(httpService.queryProphetService(any(), any())).thenReturn(responseObj);

        // Initialize Anomaly points
        List<Anomaly> anomalies1 = new ArrayList<>();
        List<Anomaly> anomalies2 = new ArrayList<>();
        Anomaly anomaly1 = new Anomaly();
        anomaly1.id = "metric";
        anomaly1.type = "point_outlier";
        anomaly1.modelName = "KSigmaModel";
        anomaly1.metricMetaData.id = "123zx23";
        anomaly1.metricMetaData.name = "metric";
        anomaly1.metricMetaData.source = "filter1, filter2";
        Float[] errors = new Float[]{9.0008904E7f, 9999989.0f, 199.99956f, 99.99989f, 7.0f};
        Float[] threshold = new Float[]{7.335708E7f, 8149973.0f, 162.99928f, 81.499725f, 5.7049866f};
        IntervalSequence is = new IntervalSequence();
        is.add(new Interval(240L, 4, errors, threshold, 9999999.99f, 11.11f));
        anomaly1.intervals = is;
        anomaly1.intervals.setLogicalIndices(0L, 60);
        anomaly1.intervals.setTimeStamps(0L, 60);
        anomalies1.add(anomaly1);
        Anomaly anomaly2 = new Anomaly();
        anomaly2.id = "metric";
        anomaly2.type = "point_outlier";
        anomaly2.modelName = "KSigmaModel";
        anomaly2.metricMetaData.id = "123zx23";
        anomaly2.metricMetaData.name = "metric";
        anomaly2.metricMetaData.source = "filter1, filter2";
        anomalies2.add(anomaly2);

        // Set up actual input/result + expected result
        List<TimeSeries> actualInput = new ArrayList<>(
                Arrays.asList(series1, series2)
        );
        List<Anomaly> actualResult = new ArrayList<>();
        List<Anomaly> expectedResult = new ArrayList<>();
        expectedResult.add(anomaly1);
        expectedResult.add(anomaly2);

        // Compute actual result
        try {
            actualResult = mockProphetAPIService.detectAnomalies(actualInput, 14);
        } catch (Exception e) {
            Assert.fail();
        }
        assertEquals(actualResult, expectedResult);
    }

    /**
     * Test generateProphetURL() generates the Prophet Service's full url correctly.
     */
    @Test
    public void testGenerateProphetURL() {
        mocks();
        when(prophetAPIService.generateProphetURL()).thenCallRealMethod();
        assertEquals(prophetAPIService.generateProphetURL(), "http://127.0.0.1:4080/forecasts");
    }

}
