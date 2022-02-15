/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.store.Attribute;

import org.apache.commons.lang.StringUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Data storer for anomaly detection jobs.
 */
@Slf4j
@Data
public class JobMetadata implements Serializable, Cloneable {

    /**
     * Serialization id for uniformity across platform.
     */
    private static final long serialVersionUID = 4L;

    /**
     * Unique job id.
     */
    @Attribute
    private Integer jobId;

    /**
     * Owner of the anomaly test.
     */
    @Attribute
    private String owner;

    /**
     * Email id of the owner.
     */
    @Attribute
    private String ownerEmail;

    /**
     * Option to have email on no-data cases.
     */
    @Attribute
    private Boolean emailOnNoData = false;

    /**
     * Slack channel for anomaly alerts.
     */
    @Attribute
    private String slackChannel;

    /**
     * User query to be stored.
     */
    @Attribute
    private String userQuery;

    /**
     * Parsed user query to be stored.
     */
    @Attribute
    private String query;

    /**
     * Test name entered by user.
     */
    @Attribute
    private String testName;

    /**
     * Description of the test.
     */
    @Attribute
    private String testDescription;

    /**
     * Superset url for the query.
     */
    @Attribute
    private String url;

    /**
     * Job status.
     */
    @Attribute
    private String jobStatus;

    /**
     * The actual next run time of the job. This parameter
     * is a timestamp in minutes since epoch (UTC time) on which
     * the job will next be executed.
     */
    @Attribute
    public Integer effectiveRunTime;

    /**
     * Report nominal time for the job execution. This parameter
     * is a timestamp in minutes since epoch (UTC time) which represents
     * the actual time for which the job will be running when the
     * job is executed.
     * <p>
     * For instance, a job executed at an effective run time of 15:00 UTC
     * may have an effective query time of 10:00 UTC, representing 5 hours of lag.
     */
    @Attribute
    public Integer effectiveQueryTime;

    /**
     * Granularity of data in the timeseries.
     */
    @Attribute
    private String granularity;

    /**
     * Range of the data to lookback.
     */
    @Attribute
    private Integer timeseriesRange;

    /**
     * Granularity range to aggregate on.
     */
    @Attribute
    private Integer granularityRange = 1;

    /**
     * Frequency of cron job.
     */
    @Attribute
    private String frequency;

    /**
     * Threshold for standard deviation on normal distribution curve.
     */
    @Attribute
    private Double sigmaThreshold;

    /**
     * Associated Druid cluster ID.
     */
    @Attribute
    private Integer clusterId;

    /**
     * Hours of lag associated with the job's cluster.
     */
    @Attribute
    private Integer hoursOfLag;

    /**
     * Timeseries Model to use for given job.
     */
    @Attribute
    private String timeseriesModel = EgadsConfig.TimeSeriesModel.OlympicModel.toString();

    /**
     * Anomaly Detection Model to use for given job.
     */
    @Attribute
    private String anomalyDetectionModel = EgadsConfig.AnomalyDetectionModel.KSigmaModel.toString();

    /**
     * Empty Constructor.
     */
    public JobMetadata() {
    }

    /**
     * Data initializer constructor.
     * @param jobMetadata JobMetadata object
     */
    public JobMetadata(JobMetadata jobMetadata) {
        this.jobId = jobMetadata.getJobId();
        this.owner = jobMetadata.getOwner();
        this.ownerEmail = jobMetadata.getOwnerEmail();
        this.emailOnNoData = jobMetadata.getEmailOnNoData();
        this.slackChannel = jobMetadata.getSlackChannel();
        this.userQuery = jobMetadata.getUserQuery();
        this.query = jobMetadata.getQuery();
        this.testName = jobMetadata.getTestName();
        this.testDescription = jobMetadata.getTestDescription();
        this.url = jobMetadata.getUrl();
        this.jobStatus = jobMetadata.getJobStatus();
        this.effectiveRunTime = jobMetadata.getEffectiveRunTime();
        this.effectiveQueryTime = jobMetadata.getEffectiveQueryTime();
        this.granularity = jobMetadata.getGranularity();
        this.timeseriesRange = jobMetadata.getTimeseriesRange();
        this.granularityRange = jobMetadata.getGranularityRange();
        this.frequency = jobMetadata.getFrequency();
        this.sigmaThreshold = jobMetadata.getSigmaThreshold();
        this.clusterId = jobMetadata.getClusterId();
        this.hoursOfLag = jobMetadata.getHoursOfLag();
        this.timeseriesModel = jobMetadata.getTimeseriesModel();
        this.anomalyDetectionModel = jobMetadata.getAnomalyDetectionModel();
    }

    /**
     * Build a job metadata object from a user query.
     * Job last run time, next run time, and update time are
     * set to empty, and the ID is set to null.
     *
     * @param userQuery user query object
     * @param query     druid query object
     */
    public JobMetadata(UserQuery userQuery, @Nullable Query query) {
        setOwner(userQuery.getOwner());
        setSlackChannel(userQuery.getSlackChannel());
        setOwnerEmail(userQuery.getOwnerEmail());
        setEmailOnNoData(userQuery.getEmailOnNoData());
        setUserQuery(userQuery.getQuery());
        setQuery(query == null ? null : query.getQueryJsonObject().toString());
        setTestName(userQuery.getTestName());
        setTestDescription(userQuery.getTestDescription());
        setUrl(userQuery.getQueryUrl());
        setJobStatus(JobStatus.CREATED.getValue());
        setGranularity(userQuery.getGranularity());
        setTimeseriesRange(userQuery.getTimeseriesRange());
        setGranularityRange(userQuery.getGranularityRange());
        setFrequency(userQuery.getFrequency());
        setSigmaThreshold(userQuery.getSigmaThreshold());
        setClusterId(userQuery.getClusterId());
        setHoursOfLag(userQuery.getHoursOfLag());
        setTimeseriesModel(userQuery.getTsModels());
        setAnomalyDetectionModel(userQuery.getAdModels());
    }

    /**
     * Method to clone the jobMetadata.
     * @return JobMetadata object
     * @throws CloneNotSupportedException cloning exception
     */
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * Method to copy the job content to new job instance.
     *
     * @param job job to be cloned
     * @return new cloned job instance
     */
    public static JobMetadata copyJob(JobMetadata job) {
        JobMetadata jobMetadata = new JobMetadata();
        try {
            jobMetadata = (JobMetadata) job.clone();
        } catch (CloneNotSupportedException e) {
            log.error("Exception while cloning the job {} : {}", job.getJobId(), e.getMessage());
        }
        jobMetadata.setEffectiveRunTime(null);
        jobMetadata.setEffectiveQueryTime(null);
        return jobMetadata;
    }

    /**
     * @return the next run time of the job formatted as a readable date
     */
    public String getFormattedNextRunTime() {
        return TimeUtils.getFormattedTimeMinutes(effectiveRunTime);
    }

    /**
     * Perform an update of this job metadata from an object
     * with the updated fields. Query is only updated
     * if the query has changed.
     *
     * @param newJob job to update with
     */
    public void update(JobMetadata newJob) {
        if (newJob.getQuery() != null) {
            setQuery(newJob.getQuery());
        }
        setOwner(newJob.getOwner());
        setSlackChannel(newJob.getSlackChannel());
        setOwnerEmail(newJob.getOwnerEmail());
        setEmailOnNoData(newJob.getEmailOnNoData());
        setUserQuery(newJob.getUserQuery());
        setTestName(newJob.getTestName());
        setTestDescription(newJob.getTestDescription());
        setUrl(newJob.getUrl());
        setGranularity(newJob.getGranularity());
        setTimeseriesRange(newJob.getTimeseriesRange());
        setGranularityRange(newJob.getGranularityRange());
        setFrequency(newJob.getFrequency());
        setSigmaThreshold(newJob.getSigmaThreshold());
        setTimeseriesModel(newJob.getTimeseriesModel());
        setAnomalyDetectionModel(newJob.getAnomalyDetectionModel());
        setClusterId(newJob.getClusterId());
        setHoursOfLag(newJob.getHoursOfLag());
    }

    /**
     * Returns true if a user query has a changed granularity,
     * frequency, hours of lag or druid cluster.
     *
     * @param userQuery user update query
     * @return true if a running job should be rescheduled
     */
    public boolean isScheduleChangeRequire(UserQuery userQuery) {
        return !getGranularity().equals(userQuery.getGranularity()) ||
               !getFrequency().equals(userQuery.getFrequency()) ||
               !getClusterId().equals(userQuery.getClusterId()) ||
               !getHoursOfLag().equals(userQuery.getHoursOfLag());
    }

    /**
     * @return true if the job status is running
     */
    public boolean isRunning() {
        return JobStatus.RUNNING.getValue().equals(getJobStatus()) || JobStatus.NODATA.getValue().equals(getJobStatus());
    }

    /**
     * @return true if the job status is NODATA
     */
    public boolean isNoData() {
        return JobStatus.NODATA.getValue().equals(getJobStatus());
    }

    /**
     * @return the generated query string with formatting
     * for display on UI
     */
    public String getPrettyQuery() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            return gson.toJson(new JsonParser().parse(query));
        } catch (Exception ignored) {
            return "Syntax error";
        }
    }

    /**
     * @return the effective query end time minus one granularity
     */
    public Integer getReportNominalTime() {
        return effectiveQueryTime - Granularity.getValue(granularity).getMinutes() * granularityRange;
    }

    /**
     * Get email comma separated string as a list.
     * @return a list
     */
    public List<String> getOwnerEmailAsList() {
        if (StringUtils.isEmpty(this.getOwnerEmail())) {
            return new ArrayList<>();
        }
        return new ArrayList<>(Arrays.asList(this.getOwnerEmail().split(Constants.COMMA_DELIMITER)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobMetadata that = (JobMetadata) o;
        return jobId == null ? that.jobId == null : jobId.equals(that.jobId);
    }

    @Override
    public int hashCode() {
        return null != jobId ? jobId.hashCode() : 1;
    }
}
