/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.scheduler.JobExecutionService;
import com.yahoo.sherlock.scheduler.SchedulerService;
/**
 * Service class to access all the services.
 */
public class ServiceFactory {

    /**
     * Method to get DetectorService instance.
     * @return DetectorService object
     */
    public DetectorService newDetectorServiceInstance() {
        return new DetectorService();
    }

    /**
     * Method to get DruidQueryService instance.
     * @return DruidQueryService object
     */
    public DruidQueryService newDruidQueryServiceInstance() {
        return new DruidQueryService();
    }

    /**
     * Method to get Scheduler for Scheduler instance.
     * @return DetectorService object
     */
    public SchedulerService newSchedulerServiceInstance() {
        return SchedulerService.getInstance();
    }

    /**
     * Method to get EgadsService instance.
     * @return EgadsService object
     */
    protected EgadsService newEgadsServiceInstance() {
        return new EgadsService();
    }

    /**
     * Method to get EmailService instance.
     * @return EmailService object
     */
    public EmailService newEmailServiceInstance() {
        return new EmailService();
    }

    /**
     * Method to get HttpService instance.
     * @return HttpService object
     */
    public HttpService newHttpServiceInstance() {
        return new HttpService();
    }

    /**
     * Method to get TimeSeriesParserService instance.
     * @return TimeSeriesParserService object
     */
    public TimeSeriesParserService newTimeSeriesParserServiceInstance() {
        return new TimeSeriesParserService();
    }

    /**
     * @return a job execution service instance
     */
    public JobExecutionService newJobExecutionService() {
        return new JobExecutionService();
    }

    /**
     * @return a discovery service instance
     */
    public DiscoService newDiscoService() {
        return new DiscoService();
    }
}
