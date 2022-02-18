/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.egads.data.Anomaly;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AnomalyReportTest {

    private static String formatHrs(int hrs) {
        return TimeUtils.getTimeFromSeconds(hrs * 3600, Constants.TIMESTAMP_FORMAT);
    }

    @Test
    public void testGetFormattedTimestampsReturnsSingleAndRange() {
        String timestampStr = "337,554,557:560,3000,3200:3300";
        AnomalyReport rep = new AnomalyReport();
        rep.setJobFrequency("hour");
        rep.setAnomalyTimestamps(timestampStr);
        String result = rep.getFormattedAnomalyTimestamps();
        String expected = String.format(
                "%s%n%s%n%s to %s%n%s%n%s to %s",
                formatHrs(337),
                formatHrs(554),
                formatHrs(557),
                formatHrs(560),
                formatHrs(3000),
                formatHrs(3200),
                formatHrs(3300)
        );
        assertEquals(result, expected);
    }

    @Test
    public void testGetAnomalyTimestampHoursReturnsSinglesAndPairs() {
        String timestampStr = "337@23,554@-40,557:560@45,3000@-1,3200:3300@90";
        AnomalyReport rep = new AnomalyReport();
        rep.setAnomalyTimestamps(timestampStr);
        List<int[]> result = rep.getAnomalyTimestampsHours();
        int[][] resArr = result.toArray(new int[0][]);
        int[][] expected = {
                {337, 0},
                {554, 0},
                {557, 560},
                {3000, 0},
                {3200, 3300}
        };
        String expectedDeviationString = "23,-40,45,-1,90";
        for (int i = 0; i < expected.length; i++) {
            assertEquals(resArr[i][0], expected[i][0]);
            assertEquals(resArr[i][1], expected[i][1]);
        }
        assertEquals(rep.getDeviationString(), expectedDeviationString);
    }

    @Test
    public void testSetAnomalyTimestampsFromIntervalsCorrectlySetsValues() {
        String expected = "337@0,554@-100,557:560@0,3000@-100,3200:3300@3";
        int[][] source = {
                {337, 337},
                {554, 0},
                {557, 560},
                {3000, 0},
                {3200, 3300}
        };
        Anomaly.IntervalSequence seq = new Anomaly.IntervalSequence();
        for (int[] pair : source) {
            Anomaly.Interval interval = new Anomaly.Interval();
            interval.startTime = pair[0] * 3600;
            interval.endTime = (long) pair[1] * 3600;
            interval.expectedVal = (float) interval.startTime;
            interval.actualVal = (float) interval.endTime;
            seq.add(interval);
        }
        // Make some end times null
        seq.get(1).endTime = null;
        AnomalyReport rep = new AnomalyReport();
        rep.setJobFrequency(Triggers.HOUR.toString());
        rep.setAnomalyTimestampsFromInterval(seq);
        assertEquals(rep.getAnomalyTimestamps(), expected);
        seq.clear();
        AnomalyReport rep1 = new AnomalyReport();
        for (int[] pair : source) {
            Anomaly.Interval interval = new Anomaly.Interval();
            interval.startTime = pair[0] * 60;
            interval.endTime = (long) pair[1] * 60;
            interval.expectedVal = (float) interval.startTime;
            interval.actualVal = (float) interval.endTime;
            seq.add(interval);
        }
        rep1.setJobFrequency(Triggers.MINUTE.toString());
        rep1.setAnomalyTimestampsFromInterval(seq);
        assertEquals(rep1.getAnomalyTimestamps(), expected);
    }

    @Test
    public void testAnomalyReportEquals() {
        AnomalyReport rep = new AnomalyReport();
        Set<AnomalyReport> reps = Collections.singleton(rep);
        assertTrue(rep.equals(reps.iterator().next()));
        assertFalse(rep.equals(null));
        assertFalse(rep.equals("lol"));
        rep.setUniqueId("1");
        AnomalyReport r0 = new AnomalyReport();
        r0.setUniqueId("1");
        assertTrue(r0.equals(rep));
    }

    @Test
    public void testAnomalyReportHashcode() {
        AnomalyReport rep = new AnomalyReport();
        rep.setUniqueId("1");
        assertEquals(rep.hashCode(), "1".hashCode());
    }

    @Test
    public void testParameterConstructor() {
        AnomalyReport rep = new AnomalyReport(
                "uniqueId",
                "metricName",
                "groupByFilters",
                "anomalyTimestamps",
                "queryUrl",
                12345,
                1,
                "jobFrequency",
                "status",
                "model",
                "3.0",
                "xyz",
                "series"
        );
        assertEquals(rep.getUniqueId(), "uniqueId");
        assertEquals(rep.getMetricName(), "metricName");
        assertEquals(rep.getGroupByFilters(), "groupByFilters");
        assertEquals(rep.getAnomalyTimestamps(), "anomalyTimestamps");
        assertEquals(rep.getQueryURL(), "queryUrl");
        assertEquals(rep.getReportQueryEndTime(), (Integer) 12345);
        assertEquals(rep.getJobId(), (Integer) 1);
        assertEquals(rep.getJobFrequency(), "jobFrequency");
        assertEquals(rep.getStatus(), "status");
        assertEquals(rep.getModelName(), "model");
        assertEquals(rep.getModelParam(), "3.0");
        assertEquals(rep.getTestName(), "xyz");

    }

    @Test
    public void testCreateReportAndSetAnomalyTimestampsFromBytes() {
        Anomaly anomaly = new Anomaly();
        Anomaly.IntervalSequence seq = new Anomaly.IntervalSequence();
        Anomaly.Interval interval = new Anomaly.Interval();
        interval.startTime = 60L;
        interval.endTime = 120L;
        interval.expectedVal = (float) interval.startTime;
        interval.actualVal = (float) interval.endTime;
        Anomaly.Interval interval2 = new Anomaly.Interval();
        interval2.startTime = 0L;
        interval2.endTime = 120L;
        interval2.expectedVal = (float) 60;
        interval2.actualVal = (float) interval2.endTime;
        seq.add(interval);
        seq.add(interval2);
        anomaly.intervals = seq;
        anomaly.metricMetaData.id = "123";
        anomaly.metricMetaData.name = "m1";
        anomaly.metricMetaData.source = "s1";
        JobMetadata jobMetadata = mock(JobMetadata.class);
        when(jobMetadata.getReportNominalTime()).thenReturn(123);
        when(jobMetadata.getFrequency()).thenReturn("minute");
        when(jobMetadata.getJobId()).thenReturn(1);
        AnomalyReport report = AnomalyReport.createReport(anomaly, jobMetadata);
        assertEquals(report.getJobId(), (Integer) 1);
        assertEquals(report.getMetricName(), "m1");
        assertEquals(report.getUniqueId(), "123");
        assertEquals(report.getGroupByFilters(), "s1");
        assertEquals(report.getAnomalyTimestamps(), "1:2@100,0:2@100");

        List<int[]> timestamps = report.getAnomalyTimestampsHours();
        byte[][] startTime = new byte[timestamps.size()][];
        byte[][] endTime = new byte[timestamps.size()][];
        for (int i = 0; i < timestamps.size(); i++) {
            int[] timestamp = timestamps.get(i);
            startTime[i] = NumberUtils.toBytesCompressed(timestamp[0]);
            if (timestamp[1] != 0 && timestamp[1] != timestamp[0]) {
                endTime[i] = NumberUtils.toBytesCompressed(timestamp[1]);
            }
        }
        AnomalyReport report1 = new AnomalyReport();
        report1.setDeviationString("23,56");
        report1.setAnomalyTimestampsFromBytes(startTime, endTime);
        assertEquals(report1.getAnomalyTimestamps(), "1:2@23,0:2@56");
        report1.setDeviationString(null);
        report1.setAnomalyTimestampsFromBytes(new byte[timestamps.size()][], endTime);
        assertEquals(report1.getAnomalyTimestamps(), "2@null,2@null");
    }

    @Test
    public void testGetMetricInfoAndModelInfo() {
        AnomalyReport report = new AnomalyReport();
        report.setMetricName("m1");
        report.setTestName("test1");
        report.setModelName("model1");
        report.setModelParam("3.0");
        StringJoiner joiner = new StringJoiner(Constants.NEWLINE_DELIMITER);
        joiner.add("Metric: " + "m1");
        joiner.add("Anomaly test: " + "test1");
        Assert.assertEquals(report.getMetricInfo(), joiner.toString());
        joiner = new StringJoiner(Constants.NEWLINE_DELIMITER);
        joiner.add("Model: " + "model1");
        joiner.add("Params: " + "3.0");
        Assert.assertEquals(report.getModelInfo(), joiner.toString());
    }
}
