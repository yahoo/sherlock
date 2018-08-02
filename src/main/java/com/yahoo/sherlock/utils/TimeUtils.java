/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.settings.Constants;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Helper class for date-time format.
 */
public class TimeUtils {

    /**
     * Method to get the current date in given date format.
     *
     * @param format date format
     * @return current date in given date format
     */
    public static String getCurrentTime(String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(new Date());
    }

    /**
     * Method to get the date in given date format from timestamp.
     *
     * @param seconds timestamp in seconds
     * @param format  date format
     * @return date in given date format
     */
    public static String getTimeFromSeconds(long seconds, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(seconds * 1000);
    }

    /**
     * Method to get the date in seconds from date and its format.
     *
     * @param stringDate input date
     * @param format     format of input date
     * @return seconds as a string
     * @throws ParseException parse exception
     */
    public static String getTimeInSeconds(String stringDate, String format) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = dateFormat.parse(stringDate);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(date);
        return String.valueOf(calendar.getTimeInMillis() / 1000);
    }

    /**
     * @return the current timestamp in minutes as a string
     */
    public static String getCurrentTimeInMinutes() {
        return String.valueOf(getTimestampMinutes());
    }

    /**
     * @return the current Unix timestamp in seconds
     */
    public static long getTimestampSeconds() {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() / 1000L;
    }

    /**
     * @return the current Unix timestamp in minutes
     */
    public static long getTimestampMinutes() {
        return getTimestampSeconds() / 60L;
    }

    /**
     * Get a readable date from a string timestamp value.
     *
     * @param timestampStr the timestamp string
     * @return a readable date
     */
    public static String getFormattedTime(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty() || !NumberUtils.isInteger(timestampStr)) {
            return "";
        }
        return getTimeFromSeconds(Long.parseLong(timestampStr), Constants.TIMESTAMP_FORMAT_NO_SECONDS);
    }

    /**
     * Get a readable date from a string timestamp value in minutes.
     *
     * @param timestampStr timestamp string
     * @return a readable date
     */
    public static String getFormattedTimeMinutes(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty() || !NumberUtils.isInteger(timestampStr)) {
            return "";
        }
        return getTimeFromSeconds(Long.parseLong(timestampStr) * 60, Constants.TIMESTAMP_FORMAT_NO_SECONDS);
    }

    /**
     * Format a timestamp in minutes as a readable date.
     *
     * @param timestampMinutes minutes since epoch
     * @return readable date
     */
    public static String getFormattedTimeMinutes(Integer timestampMinutes) {
        if (timestampMinutes == null) {
            return "";
        }
        return getTimeFromSeconds(((long) timestampMinutes) * 60L, Constants.TIMESTAMP_FORMAT_NO_SECONDS);
    }

    /**
     * Method to convert timestamp in seconds to timestamp to hours.
     *
     * @param seconds timestamp in seconds
     * @return timestamp to hours
     */
    public static Long getTimestampInHoursFromSeconds(Long seconds) {
        return seconds / 3600;
    }

    /**
     * Method to convert timestamp in hours to timestamp in seconds.
     *
     * @param hours timestamp in hours
     * @return timestamp in seconds
     */
    public static Long getTimestampInSecondsFromHours(Long hours) {
        return hours * 3600;
    }

    /**
     * Method to convert timestamp in seconds to timestamp to minutes.
     *
     * @param seconds timestamp in seconds
     * @return timestamp to minutes
     */
    public static Long getTimestampInMinutesFromSeconds(Long seconds) {
        return seconds / 60;
    }

    /**
     * Method to convert timestamp in minutes to timestamp in seconds.
     *
     * @param minutes timestamp in minutes
     * @return timestamp in seconds
     */
    public static Long getTimestampInSecondsFromMinutes(Long minutes) {
        return minutes * 60;
    }

    /**
     * Parses the date time as a zoned date time
     * from a {@code datetime-local} input element.
     *
     * @param datetime date time as a string
     * @return date time as a zoned date time
     */
    public static ZonedDateTime parseDateTime(String datetime) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
            LocalDateTime localDate = LocalDateTime.parse(datetime, formatter);
            return ZonedDateTime.of(localDate, ZoneOffset.UTC);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Convert a timestamp in minutes to a ZonedDateTime instance.
     *
     * @param timestampMinutes timestamp in minutes
     * @return Zoned date time of the timestamp
     */
    public static ZonedDateTime zonedDateTimeFromMinutes(long timestampMinutes) {
        long timestampMillis = timestampMinutes * Constants.MILLISECONDS_IN_MINUTE;
        Instant instant = Instant.ofEpochMilli(timestampMillis);
        return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    /**
     * Get the timestamp in seconds of a {@code ZonedDateTime}.
     *
     * @param date the zoned date time
     * @return the corresponding timestamp in seconds
     */
    public static long zonedDateTimestamp(ZonedDateTime date) {
        return date.toInstant().toEpochMilli() / 1000L;
    }

    /**
     * Get the timestamp in seconds of a {@code ZonedDateTime} as a string.
     *
     * @param date the zoned date time
     * @return the corresponding timestamp in seconds as a string
     */
    public static String getDateTimestamp(ZonedDateTime date) {
        return String.valueOf(zonedDateTimestamp(date));
    }

    /**
     * Method to add month in given timestamp.
     * @param timestamp timestamp in minutes
     * @param n number of months to add
     * @return timestamp with added months
     */
    public static Integer addMonth(Integer timestamp, int n) {
        ZonedDateTime zonedDateTime = TimeUtils.zonedDateTimeFromMinutes(timestamp);
        zonedDateTime = zonedDateTime.plusMonths(n);
        return (int) TimeUtils.zonedDateTimestamp(zonedDateTime) / 60;
    }

    /**
     * Method to subtract month from given timestamp.
     * @param timestamp timestamp in minutes
     * @param n number of months to subtract
     * @return timestamp with subtracted months
     */
    public static Integer subtractMonth(Integer timestamp, int n) {
        ZonedDateTime zonedDateTime = TimeUtils.zonedDateTimeFromMinutes(timestamp);
        zonedDateTime = zonedDateTime.minusMonths(n);
        return (int) TimeUtils.zonedDateTimestamp(zonedDateTime) / 60;
    }
}
