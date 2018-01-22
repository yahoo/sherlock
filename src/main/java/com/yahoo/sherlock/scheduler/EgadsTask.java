/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.service.DetectorService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * EGADS processing task as a {@code Runnable} so that
 * multiple EGADS processing tasks can be run at the
 * same time.
 */
@Slf4j
public class EgadsTask implements Runnable {

    /**
     * The job proxy instance.
     */
    private JobMetadata proxyJob;
    /**
     * the effective report generation time.
     */
    private Integer reportNominalTime;
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
     * @param reportNominalTime the effective report generation time
     * @param timeSeriesList the time series data
     * @param detectorService detector service to use
     * @param executionService execution service to use
     */
    public EgadsTask(
        JobMetadata job,
        Integer reportNominalTime,
        List<TimeSeries> timeSeriesList,
        DetectorService detectorService,
        JobExecutionService executionService
    ) {
        this.proxyJob = JobMetadata.copyJob(job);
        this.reportNominalTime = reportNominalTime;
        this.timeSeriesList = timeSeriesList;
        this.detectorService = detectorService;
        this.executionService = executionService;
        this.reports = null;
    }

    /**
     * Run the job. The job will perform the EGADS
     * detection on its data and keep a reference
     * to its reports.
     */
    @Override
    public void run() {
        boolean error = false;
        List<Anomaly> anomalies = new ArrayList<>();
        List<AnomalyReport> reports = new ArrayList<>();
        try {
            proxyJob.setEffectiveQueryTime(reportNominalTime);
            Integer effectiveEndTime = reportNominalTime - Granularity.getValue(proxyJob.getGranularity()).getMinutes();
            anomalies = detectorService.runDetection(timeSeriesList, proxyJob.getSigmaThreshold(), null, effectiveEndTime, proxyJob.getFrequency());
            reports = executionService.getReports(anomalies, proxyJob);
        } catch (Exception e) {
            log.info("Error in egads job!", e);
        }
        if (reports.isEmpty()) {
            reports.add(executionService.getSingletonReport(proxyJob, anomalies.size() > 0 ? anomalies.get(0) : null));
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
