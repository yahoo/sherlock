/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.egads.control.AnomalyDetector;
import com.yahoo.egads.control.ModelAdapter;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.models.adm.AnomalyDetectionModel;
import com.yahoo.egads.models.tsmm.TimeSeriesModel;
import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.exception.SherlockException;

import com.yahoo.sherlock.query.DetectorConfig;
import com.yahoo.sherlock.service.DetectorAPIService;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for missing data filling util in egads.
 */
@Slf4j
public class EgadsUtilsTest {

    private static TimeSeries series;
    @BeforeMethod
    public void setUp() throws Exception {
        series = new TimeSeries();
        series.meta.id = "123zx23";
        series.meta.name = "metric";
        series.meta.source = "filter1, filter2";
        series.append(3600L, 11.11f);
        series.append(7200L, 22.22f);
        series.append(10800L, 33.33f);
        series.append(14400L, 55.55f);
        series.append(18000L, 33.33f);
        series.append(21600L, 77.77f);
        series.append(25200L, 99.99f);
        series.append(36000L, 100.99f);
    }

    /**
     * Method to test fillMissingData().
     *
     * @throws Exception exception
     */
    @Test
    public void testFillMissingData() throws Exception {
        Properties p = new Properties();
        p.setProperty("AGGREGATION", "1");
        p.setProperty("FILL_MISSING", "1");
        TimeSeries ts = EgadsUtils.fillMissingData(series, p);
        ts.data.forEach(datapoint -> {
                Timestamp stamp = new Timestamp(datapoint.time * 1000);
                Date date = new Date(stamp.getTime());
                log.info(date.toString() + "  " + datapoint.value);
            }
        );
    }

    @Test(expectedExceptions = SherlockException.class)
    public void testFillMissingDataUnequalPeriodFrequency() throws SherlockException {
        TimeSeries ts = mock(TimeSeries.class);
        when(ts.mostFrequentPeriod()).thenReturn(0L);
        EgadsUtils.fillMissingData(ts, 1, 1);
    }

    /**
     * Tests method getAnomalyDetector() retrieves the correct Egads Anomaly Detection Model
     * according to the given property.
     */
    @Test
    public void testGetAnomalyDetector() {
        Properties p = DetectorConfig.create().buildDefault().asProperties();
        // set "THRESHOLD" to prevent NaiveModel from failure
        p.setProperty(DetectorAPIService.THRESHOLD, "mape#10,mase#15");
        for (String name : DetectorConfig.AnomalyDetectionModel.getAllValues()) {
            p.setProperty(DetectorConfig.AD_MODEL, name);
            AnomalyDetector ad = EgadsUtils.getAnomalyDetector(series, p);
            ArrayList<AnomalyDetectionModel> adModels = (ArrayList<AnomalyDetectionModel>) TestUtilities.obtain(ad, ad.getClass(), "models");
            Assert.assertEquals(adModels.get(0).getModelName(), name);
        }
    }

    /**
     * Tests method getTSModel() retrieves the correct Egads Time Series Forecasting Model
     * according to the given property.
     */
    @Test
    public void testGetTSModel() {
        Properties p = DetectorConfig.create().buildDefault().asProperties();
        for (String name : DetectorConfig.TimeSeriesModel.getAllEgadsValues()) {
            p.setProperty("TS_MODEL", name);
            ModelAdapter ma = EgadsUtils.getTSModel(series, p);
            ArrayList<TimeSeriesModel> tsModels = (ArrayList<TimeSeriesModel>) TestUtilities.obtain(ma, ma.getClass(), "models");
            Assert.assertEquals(tsModels.get(0).getModelName(), name);
        }
    }
}
