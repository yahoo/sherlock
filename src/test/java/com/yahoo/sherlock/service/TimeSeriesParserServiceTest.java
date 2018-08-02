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

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.query.Query;
import com.yahoo.egads.data.TimeSeries;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.List;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for time-series service class.
 */
public class TimeSeriesParserServiceTest {

    private JsonArray jsonArray;
    private Query query;

    @BeforeMethod
    public void setUp() throws Exception {
        Gson gson = new Gson();
        String druidResponse = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_valid_response_2.json")));
        jsonArray = gson.fromJson(druidResponse, JsonArray.class);
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_2.json")));
        JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        query = new Query(queryJsonObject, 123, 1234, Granularity.HOUR, 1);
    }

    @Test
    public void testParseTimeSeries() throws Exception {
        TimeSeriesParserService tsps = mock(TimeSeriesParserService.class);
        doCallRealMethod().when(tsps).parseTimeSeries(jsonArray, query);
        when(tsps.isValidTimeSeries(query)).thenReturn(timeSeries -> true);
        List<TimeSeries> timeSeries = tsps.parseTimeSeries(jsonArray, query);
        Assert.assertEquals(timeSeries.size(), jsonArray.get(0).getAsJsonObject().getAsJsonArray("result").size());
        for (int i = 0; i < timeSeries.size(); i++) {
            Assert.assertEquals(timeSeries.get(i).size(), jsonArray.size());
        }
    }

    @Test
    public void testExceptions() {
        // test null druid response
        try {
            new TimeSeriesParserService().parseTimeSeries(null, query);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Null druid response!");
        }
    }

    private static float[][] getValuesFor(List<TimeSeries>[] results, int k, int sourcesSize, int intervalSize) {
        float[][] values = new float[sourcesSize][];
        for (int i = 0; i < sourcesSize; i++) {
            values[i] = new float[intervalSize];
        }
        for (int i = 0; i < sourcesSize; i++) {
            for (int j = 0; j < intervalSize; j++) {
                Assert.assertEquals(intervalSize, results[k].get(i).data.size());
                values[i][j] = results[k].get(i).data.get(j).value;
            }
        }
        return values;
    }

    @Test
    public void testSubSeries() throws Exception {
        CLISettings.INTERVAL_HOURS = 7;
        int granularityRange = 1;
        List<TimeSeries> sources = Lists.newArrayList(testSeries1(), testSeries2(), testSeries3());
        Assert.assertEquals(3, sources.size());
        Granularity granularity = Granularity.HOUR;
        long jobWindowStart = times[6];
        long end = times[times.length - 1];
        int fillIntervals = (int) ((end - jobWindowStart) / granularity.getMinutes());
        List<TimeSeries>[] results = new TimeSeriesParserService().subseries(sources, jobWindowStart, end, granularity,
                                                                             granularityRange, CLISettings.INTERVAL_HOURS);
        Assert.assertEquals(fillIntervals, results.length);
        for (List<TimeSeries> result : results) {
            Assert.assertEquals(result.size(), 3);
            for (TimeSeries resultSeries : result) {
                Assert.assertEquals(granularity.getIntervalsFromSettings(), resultSeries.data.size());
            }
        }
        float[][] expectedFirst = { // 0
                                    {12.6f, 15.8f, 19.4f, 1.5f, 40.5f, 16.8f, 19.4f},
                                    {1, 2, 3, 4, 5, 6, 7},
                                    {1.23f, -2.34f, 3.45f, -4.56f, 5.67f, -6.78f, 7.89f}
        };
        float[][] expectedLast = { // 17
                                   {13.0f, 14.0f, 15.0f, 3.21f, 4.32f, 5.43f, 6.54f},
                                   {18, 19, 20, 21, 22, 23, 24},
                                   {3, 4, 5, 11, -22, 33, -44}
        };
        float[][] expectedMiddle = { // 11
                                     {3.4f, 4.5f, 5.6f, 6.7f, 11.0f, 12.0f, 13.0f},
                                     {12, 13, 14, 15, 16, 17, 18},
                                     {-2, -3, -4, -5, 1, 2, 3}
        };
        checkEquals(getValuesFor(results, 0, sources.size(), granularity.getIntervalsFromSettings()), expectedFirst);
        checkEquals(getValuesFor(results, 17, sources.size(), granularity.getIntervalsFromSettings()), expectedLast);
        checkEquals(getValuesFor(results, 11, sources.size(), granularity.getIntervalsFromSettings()), expectedMiddle);
        granularityRange = 2;
        List<TimeSeries>[] resultsForGranularityRange2 = new TimeSeriesParserService().subseries(sources, jobWindowStart, end, granularity,
                                                                             granularityRange, CLISettings.INTERVAL_HOURS);

        Assert.assertEquals(fillIntervals, resultsForGranularityRange2.length);
        int intervals = granularity.getIntervalsFromSettings() - granularity.getIntervalsFromSettings() % granularityRange;
        for (List<TimeSeries> result : resultsForGranularityRange2) {
            Assert.assertEquals(result.size(), 3);
            for (TimeSeries resultSeries : result) {
                Assert.assertEquals(intervals / granularityRange, resultSeries.data.size());
            }
        }
        float[][] expectedFirst2 = { // 0
                                     {15.8f + 19.4f, 1.5f + 40.5f, 16.8f + 19.4f},
                                     {2 + 3, 4 + 5, 6 + 7},
                                     {-2.34f + 3.45f, -4.56f + 5.67f, -6.78f + 7.89f}
        };
        float[][] expectedLast2 = { // 17
                                    {14.0f + 15.0f, 3.21f + 4.32f, 5.43f + 6.54f},
                                    {19 + 20, 21 + 22, 23 + 24},
                                    {4 + 5, 11 + -22, 33 + -44}
        };
        float[][] expectedMiddle2 = { // 11
                                      {4.5f + 5.6f, 6.7f + 11.0f, 12.0f + 13.0f},
                                      {13 + 14, 15 + 16, 17 + 18},
                                      {-3 + -4, -5 + 1, 2 + 3}
        };
        checkEquals(getValuesFor(resultsForGranularityRange2, 0, sources.size(), intervals / granularityRange), expectedFirst2);
        checkEquals(getValuesFor(resultsForGranularityRange2, 17, sources.size(), intervals / granularityRange), expectedLast2);
        checkEquals(getValuesFor(resultsForGranularityRange2, 11, sources.size(), intervals / granularityRange), expectedMiddle2);
        CLISettings.INTERVAL_HOURS = 672;
    }

    private static void checkEquals(float[][] result, float[][] expected) {
        for (int i = 0; i < result.length; i++) {
            for (int j = 0; j < result[i].length; j++) {
                Assert.assertEquals(result[i][j], expected[i][j], 0.01f);
            }
        }
    }

    private static TimeSeries testSeries1() throws Exception {
        float[] values = {
            12.6f, 15.8f, 19.4f, 1.5f, 40.5f,
            16.8f, 19.4f, 11.4f, 12.5f, 22.5f,
            2.3f, 3.4f, 4.5f, 5.6f, 6.7f,
            11.0f, 12.0f, 13.0f, 14.0f, 15.0f,
            3.21f, 4.32f, 5.43f, 6.54f, 7.65f
        };
        return new TimeSeries(secTimes, values);
    }

    private static long[] times = {25135320, 25135380, 25135440, 25135500, 25135560,
                                   25135620, 25135680, 25135740, 25135800, 25135860,
                                   25135920, 25135980, 25136040, 25136100, 25136160,
                                   25136220, 25136280, 25136340, 25136400, 25136460,
                                   25136520, 25136580, 25136640, 25136700, 25136760};

    private static long[] secTimes = new long[25];

    static {
        for (int i = 0 ; i < 25; i++) {
            secTimes[i] = times[i] * 60;
        }
    }

    private static TimeSeries testSeries2() throws Exception {
        float[] values = {
            1, 2, 3, 4, 5,
            6, 7, 8, 9, 10,
            11, 12, 13, 14, 15,
            16, 17, 18, 19, 20,
            21, 22, 23, 24, 25
        };
        return new TimeSeries(secTimes, values);
    }

    private static TimeSeries testSeries3() throws Exception {
        float[] values = {
            1.23f, -2.34f, 3.45f, -4.56f, 5.67f,
            -6.78f, 7.89f, -8.90f, 9.01f, -10.12f,
            -1, -2, -3, -4, -5,
            1, 2, 3, 4, 5,
            11, -22, 33, -44, 55
        };
        return new TimeSeries(secTimes, values);
    }
}
