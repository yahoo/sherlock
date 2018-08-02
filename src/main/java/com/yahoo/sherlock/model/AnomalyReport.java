/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Triggers;
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
            Constants.WARNING,
            anomaly.modelName,
            String.valueOf(job.getSigmaThreshold()),
            job.getTestName()
        );
        report.setHasAnomaly(anomaly.intervals.size() > 0 || JobStatus.NODATA.getValue().equals(anomaly.metricMetaData.name));
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

    /** Name of the Model used for anomaly detection. */
    @Attribute
    private String modelName;

    /** Parameter values of the model. */
    @Attribute
    private String modelParam;

    /** String to represent deviation comma separated. */
    @Attribute
    private String deviationString;

    /** Anomaly test name associated with this report. **/
    @Attribute
    private String testName;

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
     * @param modelName Name of the Model used for anomaly detection
     * @param modelParam Parameter values of the model
     * @param testName Anomaly test name associated with this report
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
        String status,
        String modelName,
        String modelParam,
        String testName
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
        this.modelName = modelName;
        this.modelParam = modelParam;
        this.testName = testName;
    }

    /**
     * Decodes the hours since epoch timestamps stored in
     * the anomaly parameteres into readable timestamps joined
     * with newlines to be displayed on the front end.
     * @return readable timestamps separated by new lines
     */
    public String getFormattedAnomalyTimestamps() {
        if (anomalyTimestamps == null) {
            return "";
        }
        String[] anomalyTimes = anomalyTimestamps.split(Constants.COMMA_DELIMITER);
        StringJoiner joiner = new StringJoiner(Constants.NEWLINE_DELIMITER);
        for (String anomalyTime : anomalyTimes) {
            String[] timeAndValueSplit = anomalyTime.split(Constants.AT_DELIMITER);
            String[] interval = timeAndValueSplit[0].split(Constants.COLON_DELIMITER);
            String intervalStart = interval[0];
            String intervalEnd = interval.length > 1 ? interval[1] : null;
            if (!NumberUtils.isInteger(intervalStart)) {
                continue;
            }
            long startSeconds = jobFrequency.equals(Triggers.MINUTE.toString())
                                ? TimeUtils.getTimestampInSecondsFromMinutes(Long.parseLong(intervalStart))
                                : TimeUtils.getTimestampInSecondsFromHours(Long.parseLong(intervalStart));
            String startTime = TimeUtils.getTimeFromSeconds(startSeconds, Constants.TIMESTAMP_FORMAT);
            if (intervalEnd == null) {
                joiner.add(startTime);
            } else if (NumberUtils.isInteger(intervalEnd)) {
                long endSeconds = jobFrequency.equals(Triggers.MINUTE.toString())
                                  ? TimeUtils.getTimestampInSecondsFromMinutes(Long.parseLong(intervalEnd))
                                  : TimeUtils.getTimestampInSecondsFromHours(Long.parseLong(intervalEnd));
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
     *
     * @return a list of timestamp pairs
     */
    public List<int[]> getAnomalyTimestampsHours() {
        if (anomalyTimestamps == null) {
            return Collections.emptyList();
        }
        String[] anomalyTimes = anomalyTimestamps.split(Constants.COMMA_DELIMITER);
        StringJoiner joiner = new StringJoiner(Constants.COMMA_DELIMITER);
        List<int[]> timestamps = new ArrayList<>(anomalyTimes.length * 2);
        for (String anomalyTime : anomalyTimes) {
            String[] timeAndValueSplit = anomalyTime.split(Constants.AT_DELIMITER);
            String[] intervalStr = timeAndValueSplit[0].split(Constants.COLON_DELIMITER);
            if (!NumberUtils.isInteger(intervalStr[0])) {
                continue;
            }
            int[] interval = {Integer.parseInt(intervalStr[0]), 0};
            if (intervalStr.length > 1 && NumberUtils.isInteger(intervalStr[1])) {
                interval[1] = Integer.parseInt(intervalStr[1]);
            }
            if (timeAndValueSplit.length < 2) {
                joiner.add(null);
            } else {
                joiner.add(timeAndValueSplit[1]);
            }
            timestamps.add(interval);
        }
        this.deviationString = joiner.toString();
        return timestamps;
    }

    /**
     * Sets the anomaly timestamps string from an interval
     * that describes the start and end time in seconds since epoch.
     * This method will set the timestamp string as a comma
     * delimited list of hours/minutes since epoch.
     * @param intervals the intervals to set
     */
    public void setAnomalyTimestampsFromInterval(Anomaly.IntervalSequence intervals) {
        StringJoiner joiner = new StringJoiner(Constants.COMMA_DELIMITER);
        for (Anomaly.Interval interval : intervals) {
            long startHours = jobFrequency.equals(Triggers.MINUTE.toString()) ? TimeUtils.getTimestampInMinutesFromSeconds(interval.startTime) : TimeUtils.getTimestampInHoursFromSeconds(interval.startTime);
            long endHours = 0;
            if (interval.endTime != null && interval.endTime != 0) {
                endHours = jobFrequency.equals(Triggers.MINUTE.toString()) ? TimeUtils.getTimestampInMinutesFromSeconds(interval.endTime) : TimeUtils.getTimestampInHoursFromSeconds(interval.endTime);
            }
            String intervalStr;
            if (endHours == 0 || startHours == endHours) {
                intervalStr = String.valueOf(startHours);
            } else {
                intervalStr = String.format("%d:%d", startHours, endHours);
            }
            int percentageDeviation = (int) (((interval.actualVal - interval.expectedVal) / interval.expectedVal) * 100);
            intervalStr += Constants.AT_DELIMITER + String.valueOf(percentageDeviation);
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
        StringJoiner joiner = new StringJoiner(Constants.COMMA_DELIMITER);
        String[] deviations;
        if (deviationString == null) {
            deviations = new String[startBytes.length];
        } else {
            deviations = deviationString.split(Constants.COMMA_DELIMITER);
        }
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
                joiner.add(String.format("%d:%d", hrsStart, hrsEnd) + Constants.AT_DELIMITER + deviations[i]);
            } else {
                joiner.add(String.valueOf(hrsStart) + Constants.AT_DELIMITER + deviations[i]);
            }
        }
        setAnomalyTimestamps(joiner.toString());
    }

    /**
     * Method to return the metric and anomaly test info to display on UI.
     * @return metric info string
     */
    public String getMetricInfo() {
        StringJoiner joiner = new StringJoiner(Constants.NEWLINE_DELIMITER);
        joiner.add("Metric: " + metricName);
        if (testName != null) {
            joiner.add("Anomaly test: " + testName);
        }
        return joiner.toString();
    }

    /**
     * Method to return the model info to display on UI.
     * @return model info string
     */
    public String getModelInfo() {
        StringJoiner joiner = new StringJoiner(Constants.NEWLINE_DELIMITER);
        joiner.add("Model: " + modelName);
        joiner.add("Params: " + modelParam);
        return joiner.toString();
    }

    /**
     * Method to display deviation values on UI.
     * @return html string with deviation values
     */
    public String getFormattedDeviation() {
        if (anomalyTimestamps == null) {
            return "";
        }
        String[] anomalyTimes = anomalyTimestamps.split(Constants.COMMA_DELIMITER);
        StringJoiner joiner = new StringJoiner(Constants.HTML_LINEBREAK_DELIMITER);
        for (String anomalyTime : anomalyTimes) {
            String[] timeAndValueSplit = anomalyTime.split(Constants.AT_DELIMITER);
            if (timeAndValueSplit.length < 2 || !NumberUtils.isInteger(timeAndValueSplit[1])) {
                continue;
            }
            int deviation = Integer.valueOf(timeAndValueSplit[1]);
            if (deviation < 0) {
                joiner.add("<span style=\"color: rgba(240,0,0,0.8)\" class=\"glyphicon glyphicon-triangle-bottom\"></span><span style=\"color: rgba(240,0,0,0.8)\">" + timeAndValueSplit[1] + "%</span>");
            } else {
                joiner.add("<span style=\"color: rgba(0,200,0,0.8)\" class=\"glyphicon glyphicon-triangle-top\"></span><span style=\"color: rgba(0,200,0,0.8)\">" + timeAndValueSplit[1] + "%</span>");
            }
        }
        return joiner.toString();
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
