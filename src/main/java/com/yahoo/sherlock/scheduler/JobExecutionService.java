/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

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
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.service.TimeSeriesParserService;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Service class for job execution.
 */
@Slf4j
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
     * Create the service and grab references to the necessary
     * accessors and services.
     */
    public JobExecutionService() {
        serviceFactory = new ServiceFactory();
        druidClusterAccessor = Store.getDruidClusterAccessor();
        jobMetadataAccessor = Store.getJobMetadataAccessor();
        anomalyReportAccessor = Store.getAnomalyReportAccessor();
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
            List<Anomaly> anomalies = new ArrayList<>();
            List<AnomalyReport> reports = new ArrayList<>();
            try {
                anomalies = executeJob(job, druidClusterAccessor.getDruidCluster(job.getClusterId()));
                reports = getReports(anomalies, job);
            } catch (SherlockException | ClusterNotFoundException e) {
                log.error("Error while executing job: [{}]", job.getJobId(), e);
                unscheduleErroredJob(job);
            }
            EmailService emailService = serviceFactory.newEmailServiceInstance();
            if (reports.isEmpty()) {
                AnomalyReport report = getSingletonReport(job, anomalies.size() > 0 ? anomalies.get(0) : null);
                report.setReportQueryEndTime(job.getReportNominalTime());
                reports.add(report);
                if (report.getStatus().equals(Constants.ERROR) && CLISettings.ENABLE_EMAIL) {
                    if (!emailService.sendEmail(CLISettings.FAILURE_EMAIL, CLISettings.FAILURE_EMAIL, reports)) {
                        log.error("Error while sending failure email!");
                    }
                }
            } else {
                if (CLISettings.ENABLE_EMAIL) {
                    log.info("Emailing anomaly report.");
                    if (!emailService.sendEmail(job.getOwner(), job.getOwnerEmail(), reports)) {
                        log.error("Error while sending anomaly report email!");
                    }
                }
            }
            anomalyReportAccessor.putAnomalyReports(reports);
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
        Integer timestampMinutes = job.getEffectiveQueryTime();
        ZonedDateTime startTime = TimeUtils.zonedDateTimeFromMinutes(timestampMinutes);
        try {
            performBackfillJob(job, startTime);
        } catch (SherlockException e) {
            log.error("Error while backfilling job [{}]!", job.getJobId(), e);
        }
    }

    /**
     * Run a backfill for a provided job starting at the given time.
     * This method will backfill from the given start date to the
     * current time.
     *
     * @param job       metadata for job to backfill
     * @param startTime the start time of backfilling as a ZonedDateTime
     * @throws SherlockException if an error occurs during job execution
     */
    public void performBackfillJob(
            JobMetadata job,
            ZonedDateTime startTime
    ) throws SherlockException {
        Granularity granularity = Granularity.getValue(job.getGranularity());
        Integer intervalEndTime = granularity.getEndTimeForInterval(ZonedDateTime.now(ZoneOffset.UTC)
                .minusHours(job.getHoursOfLag()));
        Integer intervalStartTime = granularity.getEndTimeForInterval(startTime)
                - (granularity.getIntervalsFromSettings() - 1) * granularity.getMinutes();
        Query query = QueryBuilder.start()
                .startAt(intervalStartTime)
                .endAt(intervalEndTime)
                .queryString(job.getUserQuery())
                .granularity(granularity)
                .build();
        try {
            DruidCluster cluster = druidClusterAccessor.getDruidCluster(job.getClusterId());
            performBackfillJob(
                    job, cluster, query,
                    intervalStartTime,
                    intervalEndTime,
                    granularity
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
     * @param job         the job details
     * @param cluster     the druid cluster for the job
     * @param query       druid query to get the data
     * @param start       start of backfill job window
     * @param end         end of backfill job window
     * @param granularity the data granularity
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
            Granularity granularity
    ) throws SherlockException, DruidException, InterruptedException, IOException {
        log.info("Performing backfill for job [{}] for time range ({}, {})", job.getJobId(), start, end);
        log.info("Job granularity is [{}]", granularity.toString());
        DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
        TimeSeriesParserService parserService = serviceFactory.newTimeSeriesParserServiceInstance();
        JsonArray druidResponse = detectorService.queryDruid(query, cluster);
        List<TimeSeries> sourceSeries = parserService.parseTimeSeries(druidResponse, query);
        List<TimeSeries>[] fillSeriesList = parserService.subseries(sourceSeries, start, end, granularity);
        List<Thread> threads = new ArrayList<>(fillSeriesList.length);
        List<EgadsTask> tasks = new ArrayList<>(fillSeriesList.length);
        Integer singleInterval = granularity.getMinutes();
        Integer subEnd = start + granularity.getIntervalsFromSettings() * granularity.getMinutes();
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
        anomalyReportAccessor.putAnomalyReports(reports);
        log.info("Backfill is complete");
    }

    /**
     * Create a new egads task.
     *
     * @param job               the job to run
     * @param reportNominalTime the effective report generation time
     * @param series            time series data
     * @param detectorService   the detector service instance to use
     * @return an egads task
     */
    protected EgadsTask createTask(
            JobMetadata job,
            Integer reportNominalTime,
            List<TimeSeries> series,
            DetectorService detectorService
    ) {
        return new EgadsTask(job, reportNominalTime, series, detectorService, this);
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
            return detectorService.detect(
                    cluster,
                    job.getQuery(),
                    Granularity.getValue(job.getGranularity()),
                    job.getSigmaThreshold(),
                    job.getEffectiveQueryTime(),
                    job.getFrequency()
            );
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
                    job.getFrequency()
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
    public void unscheduleErroredJob(JobMetadata job) {
        try {
            serviceFactory.newSchedulerServiceInstance().stopJob(job.getJobId());
            job.setJobStatus(JobStatus.ERROR.getValue());
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
    public List<AnomalyReport> getReports(List<Anomaly> anomalies, JobMetadata job) {
        // if no data was returned from druid datasource
        // set the job status to 'NODATA'
        if (anomalies.size() > 0 && anomalies.get(0).metricMetaData.source.equals(JobStatus.NODATA.getValue())) {
            job.setJobStatus(JobStatus.NODATA.getValue());
            return Lists.newArrayList(0);
        }
        List<AnomalyReport> reports = new ArrayList<>(anomalies.size());
        for (Anomaly anomaly : anomalies) {
            AnomalyReport report = AnomalyReport.createReport(anomaly, job);
            if (report.isHasAnomaly()) {
                reports.add(report);
            }
        }
        return reports;
    }

    /**
     * Get a single report, which happens if no anomalies were detected
     * or if the job errored.
     *
     * @param job     the detection job
     * @param anomaly an anomaly if there is one
     * @return a SUCCESS or ERROR report
     */
    public AnomalyReport getSingletonReport(JobMetadata job, @Nullable Anomaly anomaly) {
        AnomalyReport report = new AnomalyReport();
        report.setJobFrequency(job.getFrequency());
        report.setJobId(job.getJobId());
        report.setUniqueId(null);
        report.setQueryURL(job.getUrl());
        report.setReportQueryEndTime(job.getReportNominalTime());
        if (anomaly != null) {
            report.setUniqueId(anomaly.metricMetaData.id);
            report.setMetricName(anomaly.metricMetaData.name);
            report.setGroupByFilters(anomaly.metricMetaData.source);
        }
        if (JobStatus.ERROR.getValue().equals(job.getJobStatus())) {
            log.info("Job [{}] completed with error", job.getJobId());
            report.setStatus(Constants.ERROR);
        } else if (anomaly != null && anomaly.metricMetaData.source.equals(JobStatus.NODATA.getValue())) {
            log.info("No data returned from druid for Job [{}]", job.getJobId());
            report.setStatus(Constants.NODATA);
        } else {
            report.setStatus(Constants.SUCCESS);
        }
        return report;
    }

}
