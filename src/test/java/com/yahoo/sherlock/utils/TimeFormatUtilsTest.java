/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Test for time format util functions.
 */
public class TimeFormatUtilsTest {

    String format = "EEE dd MMM yyyy";

    /**
     * test getCurrentTime().
     * @throws Exception exception
     */
    @Test
    public void testGetCurrentTime() throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String expected = dateFormat.format(new Date());
        Assert.assertEquals(TimeUtils.getCurrentTime(format), expected);
    }

    /**
     * test getTimeFromSeconds().
     * @throws Exception exception
     */
    @Test
    public void testGetTimeFromSeconds() throws Exception {
        long seconds = 1508348470;
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String expected =  dateFormat.format(seconds * 1000);
        Assert.assertEquals(TimeUtils.getTimeFromSeconds(seconds, format), expected);
    }

    /**
     * test getTimeInSeconds().
     * @throws Exception exception
     */
    @Test
    public void testGetTimeInSeconds() throws Exception {
        String dateString = "Sun 15 Oct 2017";
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = dateFormat.parse(dateString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        Assert.assertEquals(TimeUtils.getTimeInSeconds(dateString, format), String.valueOf(calendar.getTimeInMillis() / 1000));
    }

}
