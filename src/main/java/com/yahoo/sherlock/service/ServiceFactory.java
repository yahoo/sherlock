/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

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
     * Method to get EgadsAPIService instance.
     * @return EgadsAPIService object
     */
    protected EgadsAPIService newEgadsAPIServiceInstance() {
        return new EgadsAPIService();
    }

    /**
     * Method to get ProphetAPIService instance.
     * @return ProphetAPIService object
     */
    protected ProphetAPIService newProphetAPIServiceInstance() {
        return new ProphetAPIService();
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
    protected HttpService newHttpServiceInstance() {
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
}
