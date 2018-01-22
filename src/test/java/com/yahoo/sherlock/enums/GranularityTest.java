/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import com.yahoo.sherlock.settings.CLISettings;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Date;

import static org.testng.Assert.assertEquals;

/**
 * Tests the Granularity class.
 */
public class GranularityTest {

    @Test
    public void testGetMinutes() {
        int[] vals = {60, 1440, 10080, 43800};
        Granularity[] gs = Granularity.values();
        for (int i = 0; i < 4; i++) {
            assertEquals(vals[i], gs[i].getMinutes());
        }
    }

    @Test
    public void testGetValueGranularity() {
        Assert.assertNull(Granularity.getValue(null));
        Assert.assertNull(Granularity.getValue("1234"));
        assertEquals(Granularity.DAY, Granularity.getValue("day"));
    }

    /**
     * Tests the GetValue function.
     */
    @Test
    public void testGetValue() {
        //hour
        assertEquals(Granularity.HOUR.getValue(), "PT1H");

        //day
        assertEquals(Granularity.DAY.getValue(), "P1D");

        //week
        assertEquals(Granularity.WEEK.getValue(), "P1W");

        //month
        assertEquals(Granularity.MONTH.getValue(), "P1M");
    }

    /**
     * Tests the ToString function.
     */
    @Test
    public void testToString() {
        //hour
        assertEquals(Granularity.HOUR.toString(), "hour");

        //day
        assertEquals(Granularity.DAY.toString(), "day");

        //week
        assertEquals(Granularity.WEEK.toString(), "week");

        //month
        assertEquals(Granularity.MONTH.toString(), "month");
    }

    /**
     * Tests the getAllValues function.
     */
    @Test
    public void testGetAllValues() {
        //compare all values with expected list of values
        assertEquals(Granularity.getAllValues(), Arrays.asList("hour", "day", "week", "month"));
    }

    @Test
    public void testGetIntervals() {
        assertEquals(CLISettings.INTERVAL_HOURS, Granularity.HOUR.getIntervalsFromSettings());
        assertEquals(CLISettings.INTERVAL_DAYS, Granularity.DAY.getIntervalsFromSettings());
        assertEquals(CLISettings.INTERVAL_WEEKS, Granularity.WEEK.getIntervalsFromSettings());
        assertEquals(CLISettings.INTERVAL_MONTHS, Granularity.MONTH.getIntervalsFromSettings());
    }

    @Test
    public void testGetNextGranularityStartHour() {
        Date date = new Date();
        date.setTime(15 * 60 * 1000);
        long time = Granularity.HOUR.getNextGranularityStart(date);
        assertEquals(60, time);
        date.setTime(16 * 60 * 60 * 1000 + 55 * 60 * 1000);
        time = Granularity.HOUR.getNextGranularityStart(date);
        assertEquals(17 * 60, time);
    }

    @Test
    public void testGetNextGranularityStartDay() {
        Date date = new Date();
        date.setTime(16 * 60 * 60 * 1000 + 55 * 60 * 1000);
        long time = Granularity.DAY.getNextGranularityStart(date);
        long expected = 24 * 60;
        assertEquals(time, expected);
        time = 3 * 24 * 3600 * 1000 + 14 * 3600 * 1000 + 35 * 60 * 1000;
        date.setTime(time);
        expected = 4 * 24 * 60;
        time = Granularity.DAY.getNextGranularityStart(date);
        assertEquals(time, expected);
    }

    @Test
    public void testGetNextGranularityStartWeek() {
        long time = 12 * 24 * 3600 * 1000 + 14 * 3600 * 1000 + 35 * 60 * 1000;
        Date date = new Date();
        date.setTime(time);
        long expected = 18 * 24 * 60;
        assertEquals(Granularity.WEEK.getNextGranularityStart(date), expected);
        time = expected * 60000 + 3 * 24 * 3600 * 1000;
        expected = (18 + 7) * 24 * 60;
        date.setTime(time);
        assertEquals(Granularity.WEEK.getNextGranularityStart(date), expected);
    }

}
