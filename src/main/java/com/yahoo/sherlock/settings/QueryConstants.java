/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.settings;

/**
 * Constants related to Druid queries. These constants
 * represent JSON parameter key names.
 */
public class QueryConstants {

    // Druid query service constants
    public static final String POSTAGGREGATIONS = "postAggregations";
    public static final String AGGREGATIONS = "aggregations";
    public static final String INTERVALS = "intervals";
    public static final String GRANULARITY = "granularity";

    // Query constants
    public static final String AGGREGATOR = "aggregator";
    public static final String NAME = "name";
    public static final String DIMENSIONS = "dimensions";
    public static final String DIMENSION = "dimension";
    public static final String DATE_TIME_SPLIT = "\\.";
    public static final String DATE_TIME_ZERO = "+00:00";
    public static final String PERIOD = "period";
    public static final String DATASOURCE = "dataSource";
    public static final String ORIGIN = "origin";
    public static final String TYPE = "type";
    public static final String TIMEZONE = "timeZone";
    public static final String UTC = "UTC";
}
