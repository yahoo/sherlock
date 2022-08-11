/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.MetricMeta;
import com.yahoo.egads.data.TimeSeries;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.FileAssert.fail;

public class DetectorResultTest {

    @Test
    public void testEgadsResult() {
        DetectorResult r = new DetectorResult();
        r.setAnomalies(new ArrayList<>());
        assertEquals(r.getAnomalies().size(), 0);
        assertNull(r.getTimeseries());
        assertNull(r.getForecasted());
        TimeSeries ts = new TimeSeries();
        TimeSeries.DataSequence ds = new TimeSeries.DataSequence();
        List<Anomaly> al = new ArrayList<>();
        r = new DetectorResult(al, ts, ds);
        assertEquals(r.getAnomalies(), al);
        assertEquals(r.getTimeseries(), ts);
        assertEquals(r.getForecasted(), ds);
    }

    @Test
    public void testEgadsResultPoint() {
        DetectorResult.Point p = new DetectorResult.Point();
        assertEquals(0, p.getX());
        assertEquals(null, p.getY());
        p = new DetectorResult.Point(10, 10.0f);
        assertEquals(p.getX(), 10);
        assertEquals(p.getY(), Float.valueOf(10.0f));
    }

    @Test
    public void testEgadsResultSeries() {
        DetectorResult.Series s = new DetectorResult.Series();
        assertNull(s.getValues());
        assertNull(s.getKey());
        assertNull(s.getClassed());
        s = new DetectorResult.Series(new DetectorResult.Point[3], "key", 10);
        assertEquals(s.getValues().length, 3);
        assertEquals(s.getKey(), "key");
        assertEquals(s.getClassed(), "series-10");
        s = s.index(15);
        assertEquals(s.getClassed(), "series-15");
    }

    @Test
    public void testGetBaseName() {
        TimeSeries ts = mock(TimeSeries.class);
        ts.meta = mock(MetricMeta.class);
        ts.meta.source = "abc\nggg";
        ts.meta.name = "M1";
        DetectorResult detectorResult = new DetectorResult(Collections.emptyList(), ts, new TimeSeries.DataSequence());
        String result = detectorResult.getBaseName();
        assertEquals(result, "M1; abc,ggg");
    }

    private static DetectorResult getResult(float base) {
        Anomaly a = new Anomaly();
        a.intervals = new Anomaly.IntervalSequence();
        Anomaly.Interval in = new Anomaly.Interval();
        in.startTime = 567000;
        in.actualVal = base + 50;
        List<Anomaly> anomalies = new ArrayList<>();
        anomalies.add(a);
        TimeSeries ts = new TimeSeries();
        ts.data = new TimeSeries.DataSequence();
        ts.data.add(new TimeSeries.Entry(566000, base + 40));
        ts.data.add(new TimeSeries.Entry(567000, base + 50));
        ts.data.add(new TimeSeries.Entry(568000, base + 60));
        ts.data.add(new TimeSeries.Entry(569000, base + 70));
        ts.meta = new MetricMeta();
        ts.meta.source = "xddd";
        TimeSeries.DataSequence ds = new TimeSeries.DataSequence();
        ds.add(new TimeSeries.Entry(565000, base));
        ds.add(new TimeSeries.Entry(566000, base));
        ds.add(new TimeSeries.Entry(567000, base));
        ds.add(new TimeSeries.Entry(568000, base));
        ds.add(new TimeSeries.Entry(569000, base));
        return new DetectorResult(anomalies, ts, ds);
    }

    @Test
    public void testGetData() {
        DetectorResult r = getResult(100);
        DetectorResult.Series[] ss = r.getData();
        assertEquals(ss.length, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(ss[i].getValues().length, 5);
        }
        for (int i = 0; i < 5; i++) {
            if (i != 2) {
                assertNull(ss[2].getValues()[i].getY());
            }
        }
    }

    @Test
    public void testReorderDataException() {
        try {
            DetectorResult.reorderData(new DetectorResult.Series[2]);
        } catch (IllegalArgumentException e) {
            return;
        }
        fail();
    }

    @Test
    public void testReorderData() {
        DetectorResult.Series[] array = new DetectorResult.Series[9];
        for (int i = 0; i < 9; i++) {
            array[i] = new DetectorResult.Series();
            array[i].setKey(((Integer) (i + 1)).toString());
        }
        array = DetectorResult.reorderData(array);
        String[] expectedTraverse = {"1", "2", "4", "5", "7", "8", "3", "6", "9"};
        for (int i = 0; i < 9; i++) {
            assertEquals(array[i].getClassed(), "series-" + i);
            assertEquals(array[i].getKey(), expectedTraverse[i]);
        }
    }

    @Test
    public void testFuseResults() {
        List<DetectorResult> res = new ArrayList<DetectorResult>() {
            {
                add(getResult(120));
                add(getResult(100));
                add(getResult(10));
                add(getResult(12));
            }
        };
        DetectorResult.Series[] series = DetectorResult.fuseResults(res);
        assertEquals(series.length, 12);
    }

}
