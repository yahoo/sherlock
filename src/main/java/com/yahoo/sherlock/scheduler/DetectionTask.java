/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.DetectorConfig;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.JobExecutionService;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * EGADS processing task as a {@code Runnable} so that
 * multiple EGADS detection tasks can be run at the
 * same time.
 */
@Slf4j
public class DetectionTask implements Runnable {

    /**
     * The job proxy instance.
     */
    private JobMetadata proxyJob;
    /**
     * the effective endtime of subquery.
     */
    private Integer effectiveQueryEndTime;
    /**
     * The list of time series as the data.
     */
    private List<TimeSeries> timeSeriesList;
    /**
     * Instance of {@code DetectorService}.
     */
    private DetectorService detectorService;
    /**
     * Instance of {@code ExecutionService}.
     */
    private JobExecutionService executionService;

    /**
     * Where the task will store the resultant
     * reports after task execution.
     */
    private List<AnomalyReport> reports;

    /**
     * Create a new EGADS task.
     * @param job the job details
     * @param effectiveQueryEndTime the effective endtime of subquery
     * @param timeSeriesList the time series data
     * @param detectorService detector service to use
     * @param executionService execution service to use
     */
    public DetectionTask(
        JobMetadata job,
        Integer effectiveQueryEndTime,
        List<TimeSeries> timeSeriesList,
        DetectorService detectorService,
        JobExecutionService executionService
    ) {
        this.proxyJob = JobMetadata.copyJob(job);
        this.effectiveQueryEndTime = effectiveQueryEndTime;
        this.timeSeriesList = timeSeriesList;
        this.detectorService = detectorService;
        this.executionService = executionService;
        this.reports = null;
    }

    /**
     * Run the job. The job will perform the EGADS
     * anomaly detection on its data and keep a
     * reference to its reports.
     */
    @Override
    public void run() {
        List<Anomaly> anomalies;
        List<AnomalyReport> reports = new ArrayList<>();
        // reconstruct DetectorConfig
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsModel(this.proxyJob.getTimeseriesModel());
        config.setAdModel(this.proxyJob.getAnomalyDetectionModel());
        if (this.proxyJob.getTimeseriesFramework().equals(DetectorConfig.Framework.Prophet.toString())) {
            config.setTsFramework(DetectorConfig.Framework.Prophet.toString());
            config.setProphetGrowthModel(this.proxyJob.getProphetGrowthModel());
            config.setProphetYearlySeasonality(this.proxyJob.getProphetYearlySeasonality());
            config.setProphetWeeklySeasonality(this.proxyJob.getProphetWeeklySeasonality());
            config.setProphetDailySeasonality(this.proxyJob.getProphetDailySeasonality());
        } else {
            config.setTsFramework(DetectorConfig.Framework.Egads.toString());
        }
        try {
            proxyJob.setJobStatus(JobStatus.RUNNING.getValue());
            proxyJob.setEffectiveQueryTime(effectiveQueryEndTime);
            executionService.getAnomalyReportAccessor().deleteAnomalyReportsForJobAtTime(proxyJob.getJobId().toString(), proxyJob.getReportNominalTime().toString(), proxyJob.getFrequency());
            Granularity granularity = Granularity.getValue(proxyJob.getGranularity());
            anomalies = detectorService.runDetection(timeSeriesList, proxyJob.getSigmaThreshold(), config, proxyJob.getReportNominalTime(), proxyJob.getFrequency(), granularity, proxyJob.getGranularityRange());
            reports = executionService.getReports(anomalies, proxyJob);
        } catch (Exception e) {
            log.info("Error in egads job!", e);
        }
        if (reports.isEmpty()) {
            reports.add(executionService.getSingletonReport(proxyJob));
        }
        this.reports = reports;
    }

    /**
     * @return the resultant reports of the task
     */
    public List<AnomalyReport> getReports() {
        return reports;
    }
}
