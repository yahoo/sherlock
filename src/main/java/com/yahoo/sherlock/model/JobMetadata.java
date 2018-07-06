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
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.store.Attribute;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Data storer for anomaly detection jobs.
 */
@Slf4j
@Data
public class JobMetadata implements Serializable {

    /**
     * Build a job metadata object from a user query.
     * Job last run time, next run time, and update time are
     * set to empty, and the ID is set to null.
     *
     * @param userQuery user query object
     * @param query     druid query object
     * @return a job metadata
     */
    public static JobMetadata fromQuery(UserQuery userQuery, @Nullable Query query) {
        return new JobMetadata(
                null,
                userQuery.getOwner(),
                userQuery.getOwnerEmail(),
                userQuery.getQuery(),
                query == null ? null : query.getQueryJsonObject().toString(),
                userQuery.getTestName(),
                userQuery.getTestDescription(),
                userQuery.getQueryUrl(),
                JobStatus.CREATED.getValue(),
                null,
                null,
                userQuery.getGranularity(),
                userQuery.getGranularityRange(),
                userQuery.getFrequency(),
                userQuery.getSigmaThreshold(),
                userQuery.getClusterId(),
                0
        );
    }

    /**
     * Method to copy the job content to new job instance.
     *
     * @param job job to be cloned
     * @return new cloned job instance
     */
    public static JobMetadata copyJob(JobMetadata job) {
        return new JobMetadata(
                job.getJobId(),
                job.getOwner(),
                job.getOwnerEmail(),
                job.getUserQuery(),
                job.getQuery(),
                job.getTestName(),
                job.getTestDescription(),
                job.getUrl(),
                JobStatus.CREATED.getValue(),
                null,
                null,
                job.getGranularity(),
                job.getGranularityRange(),
                job.getFrequency(),
                job.getSigmaThreshold(),
                job.getClusterId(),
                job.getHoursOfLag()
        );
    }

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
     * Empty Constructor.
     */
    public JobMetadata() {
    }

    /**
     * Data initializer constructor.
     *
     * @param jobId              Unique job id
     * @param owner              Owner of the anomaly test
     * @param ownerEmail         Email id of the owner
     * @param userQuery          User query to be stored
     * @param query              Parsed user query to be stored
     * @param testName           Test name entered by user
     * @param testDescription    Description of the test
     * @param url                Superset url for the query
     * @param jobStatus          Job status
     * @param effectiveRunTime   Next run time of job
     * @param effectiveQueryTime Report Nominal time for job reports
     * @param granularity        Granularity of data in the timeseries
     * @param granularityRange   Granularity range to aggregate on.
     * @param frequency          Frequency of cron job
     * @param sigmaThreshold     Threshold for standard deviation
     * @param clusterId          associated Cluster ID
     * @param hoursOfLag         job hours of lag from cluster
     */
    public JobMetadata(
            @Nullable Integer jobId,
            String owner,
            String ownerEmail,
            String userQuery,
            String query,
            String testName,
            String testDescription,
            String url,
            String jobStatus,
            @Nullable Integer effectiveRunTime,
            @Nullable Integer effectiveQueryTime,
            String granularity,
            Integer granularityRange,
            String frequency,
            Double sigmaThreshold,
            Integer clusterId,
            Integer hoursOfLag
    ) {
        this.jobId = jobId;
        this.owner = owner;
        this.ownerEmail = ownerEmail;
        this.userQuery = userQuery;
        this.query = query;
        this.testName = testName;
        this.testDescription = testDescription;
        this.url = url;
        this.jobStatus = jobStatus;
        this.effectiveRunTime = effectiveRunTime;
        this.effectiveQueryTime = effectiveQueryTime;
        this.granularity = granularity;
        this.granularityRange = granularityRange;
        this.frequency = frequency;
        this.sigmaThreshold = sigmaThreshold;
        this.clusterId = clusterId;
        this.hoursOfLag = hoursOfLag;
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
        setOwnerEmail(newJob.getOwnerEmail());
        setUserQuery(newJob.getUserQuery());
        setTestName(newJob.getTestName());
        setTestDescription(newJob.getTestDescription());
        setUrl(newJob.getUrl());
        setGranularity(newJob.getGranularity());
        setGranularityRange(newJob.getGranularityRange());
        setFrequency(newJob.getFrequency());
        setSigmaThreshold(newJob.getSigmaThreshold());
    }

    /**
     * Returns true if a user query has a changed granularity,
     * frequency, or sigma threshold.
     *
     * @param userQuery user update query
     * @return true if a running job should be rescheduled
     */
    public boolean userQueryChangeSchedule(UserQuery userQuery) {
        return !getGranularity().equals(userQuery.getGranularity()) ||
                !getFrequency().equals(userQuery.getFrequency()) ||
                !getSigmaThreshold().equals(userQuery.getSigmaThreshold());
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
