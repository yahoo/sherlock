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
 * Triggers for hourly, daily, weekly and monthly cron jobs.
 */
public enum Triggers {
    HOUR, DAY, WEEK, MONTH;

    /**
     * Name of Trigger.
     */
    private final String name;

    /**
     * Initialization.
     */
    Triggers() {
        this.name = name().toLowerCase();
    }

    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Method to get all the trigger frequency values.
     * @return list of trigger frequency values
     */
    public static List<String> getAllValues() {
        return Stream.of(values()).map(Triggers::toString).collect(Collectors.toList());
    }
}
