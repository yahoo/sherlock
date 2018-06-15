/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.utils.TimeUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
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

    @Test
    public void testGetNextGranularityStartMonth() {
        long time = 40 * 24 * 3600 * 1000L + 14 * 3600 * 1000L + 35 * 60 * 1000L;
        Date date = new Date();
        date.setTime(time);
        long expected = 59 * 24 * 60;
        assertEquals(Granularity.MONTH.getNextGranularityStart(date), expected);
    }

    @Test
    public void testGetEndTimeForInterval() {
        long time = 33 * 24 * 60L + 13 * 60L + 12L; // DayOfWeek = Tuesday
        ZonedDateTime zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        //test for hour
        int expected = 33 * 24 * 60 + 13 * 60;
        assertEquals(Granularity.HOUR.getEndTimeForInterval(zonedTime), expected);
        //test for day
        expected = 33 * 24 * 60;
        assertEquals(Granularity.DAY.getEndTimeForInterval(zonedTime), expected);
        //test for week
        expected = 32 * 24 * 60;
        assertEquals(Granularity.WEEK.getEndTimeForInterval(zonedTime), expected);
        //test for month
        expected = 31 * 24 * 60;
        assertEquals(Granularity.MONTH.getEndTimeForInterval(zonedTime), expected);
        //test for week in case of 'Sunday'
        time = time - 2 * 24 * 60L; // DayOfWeek = Sunday
        zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        expected = (32 - 7) * 24 * 60;
        assertEquals(Granularity.WEEK.getEndTimeForInterval(zonedTime), expected);
    }

    @Test
    public void testSubtractIntervals() {
        long minutes = 20000000L;
        ZonedDateTime start = TimeUtils.zonedDateTimeFromMinutes(minutes);
        ZonedDateTime expected = TimeUtils.zonedDateTimeFromMinutes(minutes - Granularity.HOUR.getMinutes());
        assertEquals(Granularity.HOUR.subtractIntervals(start, 1), expected);
        expected = TimeUtils.zonedDateTimeFromMinutes(minutes - Granularity.DAY.getMinutes());
        assertEquals(Granularity.DAY.subtractIntervals(start, 1), expected);
        expected = TimeUtils.zonedDateTimeFromMinutes(minutes - Granularity.WEEK.getMinutes());
        assertEquals(Granularity.WEEK.subtractIntervals(start, 1), expected);
        expected = TimeUtils.zonedDateTimeFromMinutes(minutes).minusMonths(1L);
        assertEquals(Granularity.MONTH.subtractIntervals(start, 1), expected);
    }

    @Test
    public void testIncrementAndDecrement() {
        long minutes = 20000000L;
        ZonedDateTime zonedTime = TimeUtils.zonedDateTimeFromMinutes(minutes);
        assertEquals(Granularity.HOUR.decrement(zonedTime, 1), zonedTime.minusHours(1L));
        assertEquals(Granularity.DAY.decrement(zonedTime, 1), zonedTime.minusDays(1L));
        assertEquals(Granularity.WEEK.decrement(zonedTime, 1), zonedTime.minusWeeks(1L));
        assertEquals(Granularity.MONTH.decrement(zonedTime, 1), zonedTime.minusMonths(1L));
        assertEquals(Granularity.HOUR.increment(zonedTime, 1), zonedTime.plusHours(1L));
        assertEquals(Granularity.DAY.increment(zonedTime, 1), zonedTime.plusDays(1L));
        assertEquals(Granularity.WEEK.increment(zonedTime, 1), zonedTime.plusWeeks(1L));
        assertEquals(Granularity.MONTH.increment(zonedTime, 1), zonedTime.plusMonths(1L));
    }

    @Test
    public void testLookForwardPeriods() {
        assertEquals(Granularity.HOUR.lookForwardPeriods(), 12);
        assertEquals(Granularity.DAY.lookForwardPeriods(), 14);
        assertEquals(Granularity.WEEK.lookForwardPeriods(), 13);
        assertEquals(Granularity.MONTH.lookForwardPeriods(), 12);
    }

}
