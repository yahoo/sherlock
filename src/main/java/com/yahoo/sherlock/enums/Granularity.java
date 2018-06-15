/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;

import lombok.extern.slf4j.Slf4j;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum type specifying granularity.
 */
@Slf4j
public enum Granularity {

    HOUR("PT1H", 60), DAY("P1D", 1440), WEEK("P1W", 10080), MONTH("P1M", 43800);

    /**
     * Granularity value in druid query.
     */
    private final String value;

    /**
     * Name of Granularity.
     */
    private final String name;

    /**
     * The value of the granularity in minutes.
     */
    private final int minutes;

    /**
     * Initialization.
     *
     * @param value Druid granularity value
     */
    Granularity(String value, int minutes) {
        this.value = value;
        this.minutes = minutes;
        this.name = name().toLowerCase();
    }

    /**
     * Getter for value.
     *
     * @return value
     */
    public String getValue() {
        return value;
    }

    /**
     * Get the time in minutes.
     *
     * @return time in minutes
     */
    public int getMinutes() {
        return minutes;
    }

    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Get the size of the granularity interval
     * from the CLISettings.
     *
     * @return the number of intevals
     */
    public int getIntervalsFromSettings() {
        switch (this) {
            case HOUR:
                return CLISettings.INTERVAL_HOURS;
            case DAY:
                return CLISettings.INTERVAL_DAYS;
            case WEEK:
                return CLISettings.INTERVAL_WEEKS;
            case MONTH:
                return CLISettings.INTERVAL_MONTHS;
            default:
                return CLISettings.INTERVAL_HOURS;
        }
    }

    /**
     * Subtract granularity intervals of this type
     * from a given date time.
     *
     * @param now       the time to subtract intervals from
     * @param intervals the number of intervals to subtract
     * @return a copied ZonedDateTime with the intervals subtracted
     */
    public ZonedDateTime subtractIntervals(ZonedDateTime now, int intervals) {
        switch (this) {
            case HOUR:
                return now.minusHours(intervals);
            case DAY:
                return now.minusDays(intervals);
            case WEEK:
                return now.minusWeeks(intervals);
            case MONTH:
                return now.minusMonths(intervals);
            default:
                return now.minusHours(intervals);
        }
    }

    /**
     * Get the start time of the next granularity.
     *
     * @param date the current time
     * @return the timestamp in minutes of the next granularity start
     */
    public long getNextGranularityStart(Date date) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
        cal.setTime(date);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        switch (this) {
            case HOUR:
                cal.add(Calendar.HOUR_OF_DAY, 1);
                break;
            case DAY:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.add(Calendar.HOUR_OF_DAY, 24);
                break;
            case WEEK:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                // Want jobs to run on Mondays
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                cal.add(Calendar.WEEK_OF_YEAR, 1);
                break;
            case MONTH:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.add(Calendar.MONTH, 1);
                break;
        }
        return cal.getTimeInMillis() / Constants.MILLISECONDS_IN_MINUTE;
    }

    /**
     * Get the end time of the query interval.
     * @param date the current time
     * @return end time in minutes
     */
    public int getEndTimeForInterval(ZonedDateTime date) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
        Date nowDate = Date.from(date.toInstant());
        cal.setTime(nowDate);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        switch (this) {
            case HOUR:
                break;
            case DAY:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                break;
            case WEEK:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                // Want jobs to run on Mondays
                if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                    cal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR) - 1);
                }
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                break;
            case MONTH:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                break;
        }
        return (int) (cal.getTimeInMillis() / Constants.MILLISECONDS_IN_MINUTE);
    }

    /**
     * Increment a zoned date time by a
     * certain amount of this granularity.
     *
     * @param date   the date to increment
     * @param amount amount to increment
     * @return a copy of the date incremented
     */
    public ZonedDateTime increment(ZonedDateTime date, int amount) {
        switch (this) {
            case HOUR:
                return date.plusHours(amount);
            case DAY:
                return date.plusDays(amount);
            case WEEK:
                return date.plusWeeks(amount);
            case MONTH:
                return date.plusMonths(amount);
            default:
                return date.plusHours(amount);
        }
    }

    /**
     * Decrement a zoned date time by a certain
     * amount of this granularity.
     *
     * @param date   zoned date time to decrease
     * @param amount the number of times to decrease
     * @return copy of the zoned date time
     */
    public ZonedDateTime decrement(ZonedDateTime date, int amount) {
        switch (this) {
            case HOUR:
                return date.minusHours(amount);
            case DAY:
                return date.minusDays(amount);
            case WEEK:
                return date.minusWeeks(amount);
            case MONTH:
                return date.minusMonths(amount);
            default:
                return date.minusHours(amount);
        }
    }

    /**
     * The number of granularity periods to look forward
     * when displaying the job timeline.
     *
     * @return the number of periods
     */
    public int lookForwardPeriods() {
        switch (this) {
            case HOUR:
                // Half a day
                return 12;
            case DAY:
                // Two weeks
                return 14;
            case WEEK:
                // Three months
                return 13;
            case MONTH:
                // A year
                return 12;
            default:
                // Default
                return 12;
        }
    }

    /**
     * Method to get all the granularity values.
     *
     * @return list of granularity values
     */
    public static List<String> getAllValues() {
        return Stream.of(values()).map(Granularity::toString).collect(Collectors.toList());
    }

    /**
     * Get the granularity corresponding to a provided name.
     *
     * @param name granularity name
     * @return the corresponding granularity or null
     */
    public static Granularity getValue(String name) {
        if (name == null) {
            return null;
        }
        name = name.toLowerCase();
        for (Granularity granularity : Granularity.values()) {
            if (name.equals(granularity.name)) {
                return granularity;
            }
        }
        return null;
    }

}
