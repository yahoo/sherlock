/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.model.JsonTimeline;
import com.yahoo.sherlock.store.DBTestHelper;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void testDecodeBytesCorrectlyDecodesToInt() {
        byte[] bytes = {(byte) 53, (byte) 211, (byte) 181, (byte) 170};
        int expected = 903067050;
        int result = NumberUtils.decodeBytes(bytes);
        assertEquals(result, expected);
    }

    @Test
    public void testDecodeBytesCorrectlyDecodesSignedInt() {
        byte[] bytes = {(byte) 180, (byte) 211, (byte) 181, (byte) 170};
        int expected = -1261193814;
        int result = NumberUtils.decodeBytes(bytes);
        assertEquals(result, expected);
    }

    @Test
    public void testDecodeBytesCorrectlyDecodesZero() {
        byte[] bytes = {0};
        int expected = 0;
        int result = NumberUtils.decodeBytes(bytes);
        assertEquals(result, expected);
    }

    @Test
    public void testDecodeBytesDecodesLessThan4Bytes() {
        byte[] bytes = {(byte) 211, (byte) 181, (byte) 170};
        int expected = 13874602;
        int result = NumberUtils.decodeBytes(bytes);
        assertEquals(result, expected);
    }

    @Test
    public void testDecodeBytesDecodes1Byte() {
        byte[] bytes = {(byte) 26};
        int expected = 26;
        int result = NumberUtils.decodeBytes(bytes);
        assertEquals(expected, result);
    }

    @Test
    public void testDecodeBytesTruncatesMoreThan4Bytes() {
        byte[] bytes = {(byte) 180, (byte) 211, (byte) 181, (byte) 170, (byte) 111, (byte) 150};
        int expected = -1261193814;
        int result = NumberUtils.decodeBytes(bytes);
        assertEquals(expected, result);
    }

    @Test
    public void testMinBytesCountsMinBytes() {
        assertEquals(1, NumberUtils.minBytes(124));
        assertEquals(1, NumberUtils.minBytes(0));
        assertEquals(2, NumberUtils.minBytes(46506));
        assertEquals(3, NumberUtils.minBytes(13874602));
    }

    @Test
    public void testToBytesCompressedReducesNumberOfBytes() {
        byte[] expected = {(byte) 211, (byte) 181, (byte) 170};
        assertEquals(NumberUtils.toBytesCompressed(13874602), expected);
    }

    @Test
    public void testToBytesEncodesInteger() {
        byte[] expected = {0, (byte) 211, (byte) 181, (byte) 170};
        assertEquals(NumberUtils.toBytes(13874602, 4), expected);
    }

    @Test
    public void testGetAnomalyReportsAsTimeline() throws Exception {
        AnomalyReport anomalyReport1 = DBTestHelper.getNewReport();
        anomalyReport1.setJobId(123);
        anomalyReport1.setStatus(Constants.ERROR);
        AnomalyReport anomalyReport2 = DBTestHelper.getNewReport();
        anomalyReport2.setJobId(124);
        anomalyReport2.setStatus(Constants.WARNING);
        List<AnomalyReport> anomalyReports = new ArrayList<>();
        anomalyReports.add(anomalyReport2);
        anomalyReports.add(anomalyReport1);
        JsonTimeline jsonTimeline = new JsonTimeline();
        JsonTimeline.TimelinePoint timelinePoint1 = new JsonTimeline.TimelinePoint();
        timelinePoint1.setTimestamp(anomalyReport1.getReportQueryEndTime().toString());
        timelinePoint1.setType(anomalyReport1.getStatus());
        JsonTimeline.TimelinePoint timelinePoint2 = new JsonTimeline.TimelinePoint();
        timelinePoint2.setTimestamp(anomalyReport2.getReportQueryEndTime().toString());
        timelinePoint2.setType(anomalyReport2.getStatus());
        jsonTimeline.setFrequency(null);
        jsonTimeline.setTimelinePoints(Arrays.asList(timelinePoint2, timelinePoint1));
        Assert.assertEquals(Utils.getAnomalyReportsAsTimeline(anomalyReports), jsonTimeline);
    }

}
