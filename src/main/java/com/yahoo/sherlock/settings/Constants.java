/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.settings;

/**
 * Constants for the project.
 */
public class Constants {

    /**
     * Constant for help url.
     */
    public static final String HELP_URL = "";

    /**
     * Constant for Trigger name hour.
     */
    public static final String HOUR = "HOUR";

    /**
     * Constant for Trigger name day.
     */
    public static final String DAY = "DAY";

    /**
     * Timestamp format.
     */
    public static final String TIMESTAMP_FORMAT = "EEE dd MMM yyyy HH:mm:ss z";

    /**
     * Timestamp format without seconds.
     */
    public static final String TIMESTAMP_FORMAT_NO_SECONDS = "EEE dd MMM yyyy HH:mm z";

    /**
     * JobId key in job datamap.
     */
    public static final String JOB_ID = "jobId";

    /**
     * Frequency key in job datamap.
     */
    public static final String FREQUENCY = "frequency";

    /**
     * Constant for success response.
     */
    public static final String SUCCESS = "success";

    /**
     * Granularities key for granularity values list.
     */
    public static final String GRANULARITIES = "granularities";

    /**
     * Frequencies key for frequency values list.
     */
    public static final String FREQUENCIES = "frequencies";

    /**
     * Constant for 'deletedJobsView' key in UI params.
     */
    public static final String DELETEDJOBSVIEW = "deletedJobsView";

    /**
     * Constant for 'instantView' key in UI params.
     */
    public static final String INSTANTVIEW = "instantView";

    /**
     * Constant for 'error' key in UI params.
     */
    public static final String ERROR = "error";

    /**
     * Constant for 'version' key in UI params.
     */
    public static final String VERSION = "version";

    /**
     * Constant for 'title' key in UI params.
     */
    public static final String TITLE = "title";

    /**
     * Constant for 'project' key in UI params.
     */
    public static final String PROJECT = "project";

    /**
     * Constant for Sherlock.
     */
    public static final String SHERLOCK = "Sherlock";

    /**
     * Constant for ':id' in request.
     */
    public static final String ID = ":id";

    /**
     * Constant for ':frequency' in request.
     */
    public static final String FREQUENCY_PARAM = ":frequency";

    /**
     * Constant for 'warning'.
     */
    public static final String WARNING = "warning";

    /**
     * Constant for 'emailHtml'.
     */
    public static final String EMAIL_HTML = "emailHtml";

    /**
     * Constant for 'selectedDate'.
     */
    public static final String SELECTED_DATE = "selectedDate";

    /**
     * Constant for 'timelineDots'.
     */
    public static final String TIMELINE_POINTS = "timelinePoints";

    /**
     * Constant for 'hoursOfLag'.
     */
    public static final String HOURS_OF_LAG = "hoursOfLag";

    /**
     * Constant for 'druidCluster', when passing a list of clusters to the UI.
     */
    public static final String DRUID_CLUSTERS = "druidClusters";

    /**
     * Constant for 'emailError'.
     */
    public static final String EMAIL_ERROR = "emailError";

    /**
     * The number of milliseconds in a minute.
     */
    public static final int MILLISECONDS_IN_MINUTE = 60000;

    /**
     * The number of minutes in an hour.
     */
    public static final int MINUTES_IN_HOUR = 60;

    /**
     * The number of seconds in a minute.
     */
    public static final int SECONDS_IN_MINUTE = 60;

    /**
     * The number of hours in a day.
     */
    public static final int HOURS_IN_DAY = 24;

    /**
     * Constant for 'nodata' key in UI params.
     */
    public static final String NODATA = "nodata";

    /**
     * Delimiter constant for comma.
     */
    public static final String COMMA_DELIMITER = ",";

    /**
     * Delimiter constant for colon.
     */
    public static final String COLON_DELIMITER = ":";

    /**
     * Delimiter constant for '@'.
     */
    public static final String AT_DELIMITER = "@";

    /**
     * Delimiter constant for html line break.
     */
    public static final String HTML_LINEBREAK_DELIMITER = "<br/>";

    /**
     * Delimiter constant for new line.
     */
    public static final String NEWLINE_DELIMITER = "\n";

    /**
     * Retention time in weeks(unit is days) for redis keys.
     */
    public static final int REDIS_RETENTION_WEEKS_IN_DAYS = 14;

    /**
     * Retention time in years(unit is days) for redis keys.
     */
    public static final int REDIS_RETENTION_YEARS_IN_DAYS = 366;

    /**
     * Regex constant for whitespace.
     */
    public static final String WHITESPACE_REGEX = "\\s+";

}
