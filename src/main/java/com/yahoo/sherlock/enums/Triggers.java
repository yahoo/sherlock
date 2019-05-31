/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Triggers for instant and every minute, hourly, daily, weekly and monthly cron jobs.
 */
public enum Triggers {
    INSTANT(0), MINUTE(1), HOUR(60), DAY(1440), WEEK(10080), MONTH(43800);

    /**
     * Trigger value in minutes for job scheduling.
     */
    private final int minutes;

    /**
     * Name of Trigger.
     */
    private final String name;

    /**
     * Initialization.
     *
     * @param minutes minutes for given Trigger
     */
    Triggers(int minutes) {
        this.minutes = minutes;
        this.name = name().toLowerCase();
    }

    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Get the time in minutes.
     *
     * @return time in minutes
     */
    public int getMinutes() {
        return minutes;
    }

    /**
     * Method to get all the trigger frequency values.
     * @return list of trigger frequency values
     */
    public static List<String> getAllValues() {
        return Stream.of(values()).map(Triggers::toString).collect(Collectors.toList());
    }

    /**
     * Get the Trigger corresponding to a provided name.
     *
     * @param name trigger name
     * @return the corresponding trigger or null
     */
    public static Triggers getValue(String name) {
        if (name == null) {
            return null;
        }
        name = name.toLowerCase();
        for (Triggers triggers : Triggers.values()) {
            if (name.equals(triggers.name)) {
                return triggers;
            }
        }
        return null;
    }
}
