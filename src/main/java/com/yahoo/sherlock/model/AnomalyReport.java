/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.Attribute;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * DTO for anomaly report class.
 */
@Slf4j
@Data
public class AnomalyReport implements Serializable {

    /**
     * Create an anomaly report from an EGADs anomaly and the
     * corresponding job.
     * @param anomaly EGADs anomaly
     * @param job job metadata
     * @return an anomaly report
     */
    public static AnomalyReport createReport(Anomaly anomaly, JobMetadata job) {
        AnomalyReport report = new AnomalyReport(
            anomaly.metricMetaData.id,
            anomaly.metricMetaData.name,
            anomaly.metricMetaData.source,
            null,
            job.getUrl(),
            job.getReportNominalTime(),
            job.getJobId(),
            job.getFrequency(),
            Constants.WARNING
        );
        report.setHasAnomaly(anomaly.intervals.size() > 0);
        report.setAnomalyTimestampsFromInterval(anomaly.intervals);
        return report;
    }

    /** Serialization id for uniformity across platform. */
    private static final long serialVersionUID = 1L;

    /** Unique id of time-series. */
    @Attribute
    private String uniqueId;

    /** Metric name. */
    @Attribute
    private String metricName;

    /** Comma separated group by dimensions and their values. */
    @Attribute
    private String groupByFilters;

    /**
     * Comma separated anomaly timestamps. This field is not
     * annotated with {@code Attribute} because it is set manually.
     * */
    private String anomalyTimestamps;

    /** Url of superset query. */
    @Attribute
    private String queryURL;

    /** Date time (in minutes) of report generation.**/
    @Attribute
    private Integer reportQueryEndTime;

    /** ID of the associated job. */
    @Attribute
    private Integer jobId;

    /** Frequency of the associated job. */
    @Attribute
    private String jobFrequency;

    /** Status of the report: [WARNING, ERROR, SUCCESS, NODATA]. */
    @Attribute
    private String status;

    /** Whether this anomaly report contains an anomaly. */
    private boolean hasAnomaly;


    /**
     * Empty constructor.
     */
    public AnomalyReport() {
    }

    /**
     * Data initializer constructor.
     * @param uniqueId Unique id of time-series
     * @param metricName Metric name
     * @param groupByFilters Comma separated group by dimensions and their values
     * @param anomalyTimestamps Comma separated anomaly timestamps
     * @param queryURL Url of superset query
     * @param reportQueryEndTime time stamp showing report generation time
     * @param jobId the associated job ID
     * @param jobFrequency frequency of the job
     * @param status the anomaly status
     */
    public AnomalyReport(
        String uniqueId,
        String metricName,
        String groupByFilters,
        String anomalyTimestamps,
        String queryURL,
        Integer reportQueryEndTime,
        Integer jobId,
        String jobFrequency,
        String status
    ) {
        this.uniqueId = uniqueId;
        this.metricName = metricName;
        this.groupByFilters = groupByFilters;
        this.anomalyTimestamps = anomalyTimestamps;
        this.queryURL = queryURL;
        this.reportQueryEndTime = reportQueryEndTime;
        this.jobId = jobId;
        this.jobFrequency = jobFrequency;
        this.status = status;
    }

    /**
     * Decodes the hours since epoch timestamps stored in
     * the anomaly parameteres into readable timestamps joined
     * with newlines to be displayed on the front end.
     * @return readable timestamps separated by new lines
     */
    public String getFormattedAnomalyTimestamps() {
        String[] anomalyTimes = anomalyTimestamps.split(",");
        StringJoiner joiner = new StringJoiner("\n");
        for (String anomalyTime : anomalyTimes) {
            String[] interval = anomalyTime.split(":");
            if (!NumberUtils.isInteger(interval[0])) {
                continue;
            }
            long startSeconds = TimeUtils.getTimestampInSecondsFromHours(Long.parseLong(interval[0]));
            String startTime = TimeUtils.getTimeFromSeconds(startSeconds, Constants.TIMESTAMP_FORMAT);
            if (interval.length == 1) {
                joiner.add(startTime);
            } else if (NumberUtils.isInteger(interval[1])) {
                long endSeconds = TimeUtils.getTimestampInSecondsFromHours(Long.parseLong(interval[1]));
                String endTime = TimeUtils.getTimeFromSeconds(endSeconds, Constants.TIMESTAMP_FORMAT);
                joiner.add(String.format("%s to %s", startTime, endTime));
            }
        }
        return joiner.toString();
    }

    /**
     * The report generation time as a readable,
     * formatted string for use on the front end.
     * @return formatted date of the report generation time
     */
    public String getFormattedReportGenerationTime() {
        return TimeUtils.getFormattedTimeMinutes(reportQueryEndTime);
    }

    /**
     * Get a list of anomaly timestamps as their range
     * as a list of Long pairs. If there is no end time,
     * i.e. the end time is the same as the start time,
     * then the returned pair second value is zero.
     * <p>
     * We will use integer since the value is in hours
     * since epoch.
     * @return a list of timestamp pairs
     */
    public List<int[]> getAnomalyTimestampsHours() {
        if (anomalyTimestamps == null) {
            return Collections.emptyList();
        }
        String[] anomalyTimes = anomalyTimestamps.split(",");
        List<int[]> timestamps = new ArrayList<>(anomalyTimes.length * 2);
        for (String anomalyTime : anomalyTimes) {
            String[] intervalStr = anomalyTime.split(":");
            if (!NumberUtils.isInteger(intervalStr[0])) {
                continue;
            }
            int[] interval = {Integer.parseInt(intervalStr[0]), 0};
            if (intervalStr.length > 1 && NumberUtils.isInteger(intervalStr[1])) {
                interval[1] = Integer.parseInt(intervalStr[1]);
            }
            timestamps.add(interval);
        }
        return timestamps;
    }

    /**
     * Sets the anomaly timestamps string from an interval
     * that describes the start and end time in seconds since epoch.
     * This method will set the timestamp string as a comma
     * delimited list of hours since epoch.
     * @param intervals the intervals to set
     */
    public void setAnomalyTimestampsFromInterval(Anomaly.IntervalSequence intervals) {
        StringJoiner joiner = new StringJoiner(",");
        for (Anomaly.Interval interval : intervals) {
            long startHours = TimeUtils.getTimestampInHoursFromSeconds(interval.startTime);
            long endHours = 0;
            if (interval.endTime != null && interval.endTime != 0) {
                endHours = TimeUtils.getTimestampInHoursFromSeconds(interval.endTime);
            }
            String intervalStr;
            if (endHours == 0 || startHours == endHours) {
                intervalStr = String.valueOf(startHours);
            } else {
                intervalStr = String.format("%d:%d", startHours, endHours);
            }
            joiner.add(intervalStr);
        }
        anomalyTimestamps = joiner.toString();
    }

    /**
     * Given start bytes and end bytes from Redis, set the anomaly timestamps.
     *
     * @param startBytes bytes representing the starting times
     * @param endBytes bytes representing the ending times
     */
    public void setAnomalyTimestampsFromBytes(byte[][] startBytes, byte[][] endBytes) {
        StringJoiner joiner = new StringJoiner(",");
        for (int i = 0; i < startBytes.length; i++) {
            if (startBytes[i] == null || startBytes[i].length == 0) {
                log.error("Missing start time {} for report {}", i, getUniqueId());
                if (endBytes[i] != null && endBytes[i].length > 0) {
                    startBytes[i] = endBytes[i];
                    endBytes[i] = null;
                } else {
                    continue;
                }
            }
            int hrsStart = NumberUtils.decodeBytes(startBytes[i]);
            int hrsEnd = 0;
            if (endBytes[i] != null && endBytes[i].length > 0) {
                hrsEnd = NumberUtils.decodeBytes(endBytes[i]);
            }
            if (hrsEnd != 0 && hrsEnd != hrsStart) {
                joiner.add(String.format("%d:%d", hrsStart, hrsEnd));
            } else {
                joiner.add(String.valueOf(hrsStart));
            }
        }
        setAnomalyTimestamps(joiner.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnomalyReport report = (AnomalyReport) o;
        return uniqueId != null ? uniqueId.equals(report.uniqueId) : report.uniqueId == null;
    }

    @Override
    public int hashCode() {
        return uniqueId.hashCode();
    }
}
