/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.egads.data.Anomaly;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
        rep.setAnomalyTimestampsFromInterval(seq);
        assertEquals(rep.getAnomalyTimestamps(), expected);
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
            "3.0"
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

    }

}
