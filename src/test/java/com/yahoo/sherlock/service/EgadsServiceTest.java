/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.beust.jcommander.internal.Lists;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.query.QueryBuilderTest;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.egads.control.ProcessableObject;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Test for egads service.
 */
public class EgadsServiceTest {

    private static ProcessableObject processableObject;
    private static List<Anomaly> anomalies;
    private static TimeSeries timeseries;
    private static String tempConfig;

    private static class MockEgadsService extends EgadsService {
        @Override
        protected ProcessableObject getEgadsProcessableObject(TimeSeries timeseries) {
            return processableObject;
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        long start = 1508348470;
        long end = 1508349400;
        timeseries = new TimeSeries();
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
        anomalies = new ArrayList<>();
        Anomaly anomaly = new Anomaly();
        anomaly.metricMetaData.id = "123zx23";
        anomaly.metricMetaData.name = "metric";
        anomaly.metricMetaData.source = "filter1, filter2";
        anomaly.addInterval(start, end, 0.5F);
        anomalies.add(anomaly);
        ProcessableObject mockProcessableObject = mock(ProcessableObject.class);
        Mockito.doNothing().when(mockProcessableObject).process();
        when(mockProcessableObject.result()).thenReturn(anomalies);
        processableObject = mockProcessableObject;
        tempConfig = CLISettings.EGADS_CONFIG_FILENAME;
    }

    @AfterMethod
    public void tearDown() throws Exception {
        CLISettings.EGADS_CONFIG_FILENAME = tempConfig;
    }

    @Test
    public void testRunEGADSAndException() throws Exception {
        EgadsService egadsService = new MockEgadsService();
        Assert.assertEquals(egadsService.runEGADS(timeseries, 3.0), anomalies);
        CLISettings.EGADS_CONFIG_FILENAME = "/xxxxx.con";
        timeseries = new TimeSeries();
        timeseries.append(3600L, 11.11f);
        timeseries.append(3600L * 24L, 22.22f);
        Assert.assertEquals(egadsService.runEGADS(timeseries, 3.0), anomalies);
        timeseries = new TimeSeries();
        timeseries.append(3600L, 11.11f);
        timeseries.append(3600L * 24L * 7L, 22.22f);
        Assert.assertEquals(egadsService.runEGADS(timeseries, 3.0), anomalies);
        timeseries = new TimeSeries();
        timeseries.append(3600L, 11.11f);
        timeseries.append(3600L * 24L * 100L, 22.22f);
        Assert.assertEquals(egadsService.runEGADS(timeseries, 3.0), anomalies);
        ProcessableObject mockProcessableObject = mock(ProcessableObject.class);
        when(mockProcessableObject.result()).thenThrow(new IOException("error in egads"));
        processableObject = mockProcessableObject;
        try {
            egadsService.runEGADS(timeseries, 3.0);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "error in egads");
        }
    }

    @Test
    public void testDetectAnomaliesException() throws Exception {
        ProcessableObject mockProcessableObject = mock(ProcessableObject.class);
        when(mockProcessableObject.result()).thenThrow(new IOException("error in egads"));
        processableObject = mockProcessableObject;
        EgadsService egadsService = new MockEgadsService();
        try {
            egadsService.detectAnomalies(timeseries);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "error in egads");
        }
    }

    @Test
    public void testGetEgadsProcessableObject() throws Exception {
        EgadsService egadsService = new EgadsService();
        egadsService.preRunConfigure(3.0, Granularity.DAY, 1);
        // test egads
        egadsService.runEGADS(timeseries, 3.0);
    }

    @Test
    public void testDetectAnomaliesEgadsResult() throws Exception {
        EgadsService egads = mock(EgadsService.class);
        ProcessableObject po = mock(ProcessableObject.class);
        when(egads.getEgadsProcessableObject(any())).thenReturn(po);
        List<Anomaly> result = Lists.newArrayList(
            new Anomaly(), new Anomaly(), new Anomaly()
        );
        when(po.result()).thenReturn(result);
        when(egads.detectAnomaliesResult(any())).thenCallRealMethod();
        try {
            egads.detectAnomaliesResult(mock(TimeSeries.class));
        } catch (SherlockException e) {
            return;
        }
        fail();
    }

    @Test
    public void testConfigureDetectionWindow() throws Exception {
        EgadsService egadsService = new EgadsService();
        egadsService.configureWithDefault();
        egadsService.configureDetectionWindow(61, "hour", 1);
        Properties p = (Properties) QueryBuilderTest.getValue("p", egadsService);
        assertEquals(p.getProperty("DETECTION_WINDOW_START_TIME"), "60");
    }

}
