/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import static com.yahoo.sherlock.settings.CLISettings.NODATA_ON_FAILURE;

import com.beust.jcommander.internal.Lists;
import com.google.gson.JsonArray;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.query.QueryBuilder;
import com.yahoo.sherlock.scheduler.EgadsTask;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.TimeUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Service class for job execution.
 */
@Slf4j
@Data
public class JobExecutionService {

    /**
     * Class service factory instance.
     */
    private ServiceFactory serviceFactory;

    /**
     * Class Druid cluster accessor instance.
     */
    private DruidClusterAccessor druidClusterAccessor;

    /**
     * Class job metadata accessor instance.
     */
    private JobMetadataAccessor jobMetadataAccessor;

    /**
     * Class anomaly report accessor instance.
     */
    private AnomalyReportAccessor anomalyReportAccessor;

    /**
     * Class email metadata accessor instance.
     */
    private EmailMetadataAccessor emailMetadataAccessor;

    private SlackService slackService = new SlackService();

    /**
     * Create the service and grab references to the necessary
     * accessors and services.
     */
    public JobExecutionService() {
        serviceFactory = new ServiceFactory();
        druidClusterAccessor = Store.getDruidClusterAccessor();
        jobMetadataAccessor = Store.getJobMetadataAccessor();
        anomalyReportAccessor = Store.getAnomalyReportAccessor();
        emailMetadataAccessor = Store.getEmailMetadataAccessor();
    }

    /**
     * Execute a provided job. If an error occurs during execution,
     * log the error.
     *
     * @param job the job to execute
     */
    public void execute(JobMetadata job) {
        log.debug("Executing job [{}]", job.getJobId());
        try {
            List<Anomaly> anomalies;
            List<AnomalyReport> reports = new ArrayList<>();
            job.setJobStatus(JobStatus.RUNNING.getValue());
            try {
                anomalies = executeJob(job, druidClusterAccessor.getDruidCluster(job.getClusterId()));
                reports = getReports(anomalies, job);
            } catch (SherlockException | ClusterNotFoundException e) {
                log.error("Error while executing job: [{}]", job.getJobId(), e);
                handleFailedQuery(job);
            }
            EmailService emailService = serviceFactory.newEmailServiceInstance();
            if (reports.isEmpty()) {
                AnomalyReport report = getSingletonReport(job);
                reports.add(report);
            }
            List<String> originalEmailList = job.getOwnerEmail() == null || job.getOwnerEmail().isEmpty() ? new ArrayList<>() :
                                             Arrays.stream(job.getOwnerEmail().split(Constants.COMMA_DELIMITER)).collect(Collectors.toList());
            List<String> finalEmailList = new ArrayList<>();

            if (!CLISettings.SLACK_WEBHOOK.isEmpty()) {
                slackService.sendSlackMessage(job, reports);
            }

            if (reports.get(0).getStatus().equalsIgnoreCase(Constants.ERROR)) {
                emailService.processEmailReports(job, originalEmailList, reports);
                anomalyReportAccessor.putAnomalyReports(reports, finalEmailList);
            } else if (!((finalEmailList = emailMetadataAccessor.checkEmailsInInstantIndex(originalEmailList)).isEmpty())) {
                emailService.processEmailReports(job, finalEmailList, reports);
                originalEmailList.removeAll(finalEmailList);
                anomalyReportAccessor.putAnomalyReports(reports, originalEmailList);
            } else {
                anomalyReportAccessor.putAnomalyReports(reports, originalEmailList);
            }
        } catch (IOException e) {
            log.error("Error while putting anomaly reports to database!", e);
        }
    }


    /**
     * This method will use the job's query interval end time
     * as the start time for the backfill job.
     *
     * @param job metadata of the job to backfill
     */
    public void backfillJobFromIntervalEnd(JobMetadata job) {
        Integer timestampMinutes = job.getReportNominalTime();
        ZonedDateTime startTime = TimeUtils.zonedDateTimeFromMinutes(timestampMinutes);
        try {
            performBackfillJob(job, startTime, null);
        } catch (SherlockException e) {
            log.error("Error while backfilling job [{}]!", job.getJobId(), e);
        }
    }

    /**
     * Run a backfill for a provided job starting at the given time.
     * This method will backfill from the given start time to the end time
     * or till current time if end time is not specified.
     *
     * @param job       metadata for job to backfill
     * @param startTime the start time of backfilling as a ZonedDateTime
     * @param endTime the end time of backfilling as a ZonedDateTime
     * @throws SherlockException if an error occurs during job execution
     */
    public void performBackfillJob(
            JobMetadata job,
            ZonedDateTime startTime,
            @Nullable ZonedDateTime endTime
    ) throws SherlockException {
        Granularity granularity = Granularity.getValue(job.getGranularity());
        if (endTime == null) {
            endTime = ZonedDateTime.now(ZoneOffset.UTC).minusHours(job.getHoursOfLag());
        }
        Integer intervalEndTime = granularity.getEndTimeForInterval(endTime) + granularity.getMinutes() * (job.getGranularityRange() - 1);
        Integer jobWindowStart = granularity.getEndTimeForInterval(startTime) + granularity.getMinutes() * (job.getGranularityRange() - 1);
        if ((intervalEndTime - jobWindowStart) < granularity.getMinutes()) {
            throw new SherlockException("Backfill interval cannot be smaller than granularity!");
        }
        int intervals = job.getTimeseriesRange() == null ? granularity.getIntervalsFromSettings() : job.getTimeseriesRange();
        ZonedDateTime intervalStartTime = granularity.subtractIntervals(TimeUtils.zonedDateTimeFromMinutes(jobWindowStart), intervals, job.getGranularityRange());
        log.info("Querying druid starting from {}", intervalStartTime.toString());
        Query query = QueryBuilder.start()
                .startAt(intervalStartTime)
                .endAt(intervalEndTime)
                .queryString(job.getUserQuery())
                .granularity(granularity)
                .granularityRange(job.getGranularityRange())
                .setIsBackFillQuery(true)
                .build();
        try {
            DruidCluster cluster = druidClusterAccessor.getDruidCluster(job.getClusterId());
            performBackfillJob(
                    job, cluster, query,
                    jobWindowStart,
                    intervalEndTime,
                    granularity,
                    intervals
            );
        } catch (IOException | InterruptedException | ClusterNotFoundException | DruidException e) {
            log.info("Error occurred during backfill execution!", e);
            throw new SherlockException(e.getMessage(), e);
        }
    }

    /**
     * Perform a backfill job starting at a date and
     * then at each incremented granularity after that
     * date a certain number of times.
     *
     * @param job              the job details
     * @param cluster          the druid cluster for the job
     * @param query            druid query to get the data
     * @param start            start of backfill job window
     * @param end              end of backfill job window
     * @param granularity      the data granularity
     * @param intervals        intervals to lookback
     * @throws SherlockException    if an error occurs during processing
     * @throws DruidException       if an error occurs during quering druid
     * @throws InterruptedException if an error occurs in the thread
     * @throws IOException          if an error occurs while accessing the backend
     */
    public void performBackfillJob(
            JobMetadata job,
            DruidCluster cluster,
            Query query,
            Integer start,
            Integer end,
            Granularity granularity,
            int intervals
    ) throws SherlockException, DruidException, InterruptedException, IOException {
        log.info("Performing backfill for job [{}] for time range ({}, {})", job.getJobId(),
                 TimeUtils.getTimeFromSeconds(start * 60L, Constants.TIMESTAMP_FORMAT_NO_SECONDS),
                 TimeUtils.getTimeFromSeconds(end * 60L, Constants.TIMESTAMP_FORMAT_NO_SECONDS));
        log.info("Job granularity is [{}]", granularity.toString());
        DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
        TimeSeriesParserService parserService = serviceFactory.newTimeSeriesParserServiceInstance();
        JsonArray druidResponse = detectorService.queryDruid(query, cluster);
        List<TimeSeries> sourceSeries = parserService.parseTimeSeries(druidResponse, query);
        List<TimeSeries>[] fillSeriesList = parserService.subseries(sourceSeries, start, end, granularity, query.getGranularityRange(), intervals);
        List<Thread> threads = new ArrayList<>(fillSeriesList.length);
        List<EgadsTask> tasks = new ArrayList<>(fillSeriesList.length);
        Integer singleInterval = granularity.getMinutes();
        Integer subEnd = start + singleInterval;
        for (List<TimeSeries> fillSeries : fillSeriesList) {
            EgadsTask task = createTask(
                    job,
                    subEnd,
                    fillSeries,
                    detectorService
            );
            tasks.add(task);
            Thread thread = new Thread(task);
            thread.start();
            threads.add(thread);
            subEnd += singleInterval;
        }
        List<AnomalyReport> reports = new ArrayList<>();
        for (int i = 0; i < threads.size(); i++) {
            threads.get(i).join();
            reports.addAll(tasks.get(i).getReports());
        }
        anomalyReportAccessor.putAnomalyReports(reports, new ArrayList<>());
        log.info("Backfill is complete");
    }

    /**
     * Create a new egads task.
     *
     * @param job               the job to run
     * @param effectiveQueryEndTime  the effective endtime of subquery
     * @param series            time series data
     * @param detectorService   the detector service instance to use
     * @return an egads task
     */
    protected EgadsTask createTask(
            JobMetadata job,
            Integer effectiveQueryEndTime,
            List<TimeSeries> series,
            DetectorService detectorService
    ) {
        return new EgadsTask(job, effectiveQueryEndTime, series, detectorService, this);
    }

    /**
     * Execute the job by using the detector service to
     * detect anomalies. If an error occurs during execution,
     * this method attempts to unschedule the job.
     *
     * @param job     the job to execute
     * @param cluster the Druid cluster to query
     * @return a list of anomalies detected
     * @throws SherlockException if an error occurs during execution
     */
    public List<Anomaly> executeJob(JobMetadata job, DruidCluster cluster) throws SherlockException {
        log.info("Executing job [{}]", job.getJobId());
        // Detect the anomalies in the timeseries
        try {
            DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
            return detectorService.detect(cluster, job);
        } catch (Exception e) {
            log.error("Error during job execution [{}]", job.getJobId(), e);
            throw new SherlockException(e.getMessage(), e);
        }
    }

    /**
     * Execute a job with a specified query. This
     * method will use the provided query.
     *
     * @param job     job to execute
     * @param cluster druid cluster for the job
     * @param query   query to run
     * @return the list of anomalies from the job
     * @throws SherlockException if an error occurs during execution
     */
    public List<Anomaly> executeJob(JobMetadata job, DruidCluster cluster, Query query) throws SherlockException {
        return executeJob(job, cluster, query, null);
    }

    /**
     * Execute a job with the specified query and a provided
     * EGADS config object.
     *
     * @param job     job details
     * @param cluster druid cluster to use
     * @param query   Druid query to run
     * @param config  EGADS configuration
     * @return list of anomalies from the job
     * @throws SherlockException if an error occurs during execution
     */
    public List<Anomaly> executeJob(
            JobMetadata job,
            DruidCluster cluster,
            Query query,
            EgadsConfig config
    ) throws SherlockException {
        log.info("Executing job with Query [{}]", job.getJobId());
        try {
            DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
            return detectorService.detect(
                    query,
                    job.getSigmaThreshold(),
                    cluster,
                    config,
                    job.getFrequency(),
                    job.getGranularityRange()
            );
        } catch (Exception e) {
            log.error("Error during job execution [{}]", job.getJobId(), e);
            throw new SherlockException(e.getMessage(), e);
        }
    }

    /**
     * Attempt to unschedule a job that resulted in error.
     *
     * @param job the errored job
     */
    public void handleFailedQuery(JobMetadata job) {
        try {
            serviceFactory.newSchedulerServiceInstance().stopJob(job.getJobId());
            if (NODATA_ON_FAILURE) {
                job.setJobStatus(JobStatus.NODATA.getValue());
            } else {
                job.setJobStatus(JobStatus.ERROR.getValue());
            }
            jobMetadataAccessor.putJobMetadata(job);
        } catch (SchedulerException | IOException e) {
            log.error("Error while unscheduling failed job!", e);
        }
    }

    /**
     * Get reports at a specified report generation time.
     *
     * @param anomalies list of anomalies
     * @param job       job metadata
     * @return a list of reports, which may be empty
     */
    public synchronized List<AnomalyReport> getReports(List<Anomaly> anomalies, JobMetadata job) {
        if (anomalies.isEmpty()) {
            return Lists.newArrayList(0);
        }
        int  allReportWithNoData = 0;
        List<AnomalyReport> reports = new ArrayList<>(anomalies.size());
        for (Anomaly anomaly : anomalies) {
            AnomalyReport report = AnomalyReport.createReport(anomaly, job);
            if (anomaly.metricMetaData.name.equals(JobStatus.NODATA.getValue())) {
                allReportWithNoData++;
                report.setStatus(Constants.NODATA);
            }
            if (report.isHasAnomaly()) {
                reports.add(report);
            }
        }
        // if no data was returned from druid datasource
        // set the job status to 'NODATA'
        if (allReportWithNoData == anomalies.size()) {
            job.setJobStatus(JobStatus.NODATA.getValue());
            return Lists.newArrayList(0);
        }
        return reports;
    }

    /**
     * Get a single report, which happens if no anomalies were detected
     * or if the job errored.
     *
     * @param job the detection job
     * @return a SUCCESS or ERROR or NODATA report
     */
    public synchronized AnomalyReport getSingletonReport(JobMetadata job) {
        AnomalyReport report = new AnomalyReport();
        report.setJobFrequency(job.getFrequency());
        report.setJobId(job.getJobId());
        report.setQueryURL(job.getUrl());
        report.setReportQueryEndTime(job.getReportNominalTime());
        report.setTestName(job.getTestName());
        UUID uuid = UUID.randomUUID();
        report.setUniqueId(uuid.toString());
        if (JobStatus.ERROR.getValue().equals(job.getJobStatus())) {
            log.info("Job [{}] completed with error", job.getJobId());
            report.setStatus(Constants.ERROR);
        } else if (JobStatus.NODATA.getValue().equals(job.getJobStatus())) {
            log.info("No data returned from druid for Job [{}]", job.getJobId());
            report.setStatus(Constants.NODATA);
        } else {
            report.setStatus(Constants.SUCCESS);
        }
        return report;
    }

}
