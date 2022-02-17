/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.model.JobTimeline;
import com.yahoo.sherlock.model.JsonTimeline;
import com.yahoo.sherlock.model.UserQuery;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.query.QueryBuilder;
import com.yahoo.sherlock.service.JobExecutionService;
import com.yahoo.sherlock.service.SchedulerService;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.DruidQueryService;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.service.SecretProviderService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JsonDumper;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.BackupUtils;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.sherlock.utils.Utils;

import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.extern.slf4j.Slf4j;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.TemplateEngine;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static spark.Spark.halt;

/**
 * Routes logic for web requests.
 */
@Slf4j
public class Routes {

    /**
     * Default parameters.
     */
    protected static Map<String, Object> defaultParams;

    /**
     * Class Thymeleaf template engine instance.
     */
    private static TemplateEngine thymeleaf;

    private static ServiceFactory serviceFactory;
    private static SchedulerService schedulerService;
    private static AnomalyReportAccessor reportAccessor;
    private static DruidClusterAccessor clusterAccessor;
    private static JobMetadataAccessor jobAccessor;
    private static DeletedJobMetadataAccessor deletedJobAccessor;
    private static EmailMetadataAccessor emailMetadataAccessor;
    private static JsonDumper jsonDumper;
    private static JobTimeline jobTimeline;
    private static Map<String, Object> instantReportParams;

    /**
     * Initialize the default Route parameters.
     */
    public static void initParams() {
        defaultParams = new HashMap<>();
        defaultParams.put(Constants.PROJECT, CLISettings.PROJECT_NAME);
        defaultParams.put(Constants.TITLE, Constants.SHERLOCK);
        defaultParams.put(Constants.VERSION, CLISettings.VERSION);
        defaultParams.put(Constants.ERROR, null);
        defaultParams.put(Constants.INSTANTVIEW, null);
        defaultParams.put(Constants.DELETEDJOBSVIEW, null);
        defaultParams.put(Constants.GRANULARITIES, Granularity.getAllValues());
        List<String> frequencies = Triggers.getAllValues();
        frequencies.remove(Triggers.INSTANT.toString());
        defaultParams.put(Constants.FREQUENCIES, frequencies);
        defaultParams.put(Constants.EMAIL_HTML, null);
        defaultParams.put(Constants.EMAIL_ERROR, null);
        defaultParams.put(Constants.HTTP_BASE_URI, CLISettings.HTTP_BASE_URI);
        instantReportParams = new HashMap<>(defaultParams);
    }

    /**
     * Initialize and set references to service and
     * accessor objects.
     */
    public static void initServices() {
        SecretProviderService.initSecretProvider();
        thymeleaf = new ThymeleafTemplateEngine();
        serviceFactory = new ServiceFactory();
        schedulerService = serviceFactory.newSchedulerServiceInstance();
        // Grab references to the accessors, initializing them
        reportAccessor = Store.getAnomalyReportAccessor();
        clusterAccessor = Store.getDruidClusterAccessor();
        jobAccessor = Store.getJobMetadataAccessor();
        deletedJobAccessor = Store.getDeletedJobMetadataAccessor();
        emailMetadataAccessor = Store.getEmailMetadataAccessor();
        jsonDumper = Store.getJsonDumper();
        schedulerService.instantiateMainScheduler();
        schedulerService.startMainScheduler();
        schedulerService.startEmailSenderScheduler();
        schedulerService.startBackupScheduler();
        jobTimeline = new JobTimeline();
    }

    /**
     * Inititialize the routes default settings.
     *
     * @throws SherlockException exception in intialization
     */
    public static void init() throws SherlockException {
        initParams();
        initServices();
    }

    /**
     * Method called on root endpoint.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return template view route to home page
     */
    public static ModelAndView viewHomePage(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        return new ModelAndView(params, "homePage");
    }

    /**
     * Method called upon flash query request from user.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView viewInstantAnomalyJobForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // set instant form view
        params.put(Constants.INSTANTVIEW, "true");
        params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
        params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
        params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
        params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
        params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
        params.put(Constants.MINUTE, Constants.MAX_MINUTE);
        params.put(Constants.HOUR, Constants.MAX_HOUR);
        params.put(Constants.DAY, Constants.MAX_DAY);
        params.put(Constants.WEEK, Constants.MAX_WEEK);
        params.put(Constants.MONTH, Constants.MAX_MONTH);
        params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
        params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
        try {
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        } catch (IOException e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.DRUID_CLUSTERS, new ArrayList<>());
        }
        return new ModelAndView(params, "jobForm");
    }

    /**
     * Method called upon flash query request from user.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView debugInstantJobReport(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // set instant form view
        params.put(Constants.INSTANTVIEW, "false");
        try {
            log.info("Getting instant job from database.");
            JobMetadata job = jobAccessor.getJobMetadata(request.params(Constants.ID));
            List<DruidCluster> druidClusters = clusterAccessor.getDruidClusterList();
            params.put("job", job);
            params.put(Constants.DRUID_CLUSTERS, druidClusters);
            params.put(Constants.TITLE, "Instant Job Details");
            params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
            params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
            params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
            params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
            params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
            params.put(Constants.MINUTE, Constants.MAX_MINUTE);
            params.put(Constants.HOUR, Constants.MAX_HOUR);
            params.put(Constants.DAY, Constants.MAX_DAY);
            params.put(Constants.WEEK, Constants.MAX_WEEK);
            params.put(Constants.MONTH, Constants.MAX_MONTH);
            params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
            params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
            boolean isClusterPresent = druidClusters.stream().anyMatch(c -> c.getClusterId().equals(job.getClusterId()));
            params.put(Constants.IS_CLUSTER_PRESENT, isClusterPresent);
            if (!isClusterPresent) {
                log.warn("Cluster ID {} not present for job ID {}", job.getClusterId(), job.getJobId());
                params.put(Constants.ERROR, "No associated druid cluster for this Job! Please select one below");
            }
        } catch (Exception e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.DRUID_CLUSTERS, new ArrayList<>());
        }
        return new ModelAndView(params, "instantJobReport");
    }

    /**
     * Method to display status.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView viewStatus(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // set status
        return new ModelAndView(params, "status");
    }

    /**
     * Method called upon new job form request from user.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView viewNewAnomalyJobForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Add Job");
        try {
            params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
            params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
            params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
            params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
            params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
            params.put(Constants.MINUTE, Constants.MAX_MINUTE);
            params.put(Constants.HOUR, Constants.MAX_HOUR);
            params.put(Constants.DAY, Constants.MAX_DAY);
            params.put(Constants.WEEK, Constants.MAX_WEEK);
            params.put(Constants.MONTH, Constants.MAX_MONTH);
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
            params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
            params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
        } catch (IOException e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "jobForm");
    }


    /**
     * Get the user query and generate anomaly report.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String processInstantAnomalyJob(Request request, Response response) {
        log.info("Getting user query from request.");
        Map<String, Object> params = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Anomaly Report");
        try {
            UserQuery userQuery = new Gson().fromJson(request.body(), UserQuery.class);
            log.debug("userQuery: {}", userQuery);
            // regenerate user query
            Granularity granularity = Granularity.getValue(userQuery.getGranularity());
            Integer granularityRange = userQuery.getGranularityRange();
            Integer hoursOfLag = clusterAccessor.getDruidCluster(userQuery.getClusterId().toString()).getHoursOfLag();
            Integer intervalEndTime;
            ZonedDateTime endTime = TimeUtils.parseDateTime(userQuery.getQueryEndTimeText());
            if (ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag).toEpochSecond() < endTime.toEpochSecond()) {
                intervalEndTime = granularity.getEndTimeForInterval(endTime.minusHours(hoursOfLag));
            } else {
                intervalEndTime = granularity.getEndTimeForInterval(endTime);
            }
            Query query = serviceFactory.newDruidQueryServiceInstance().build(userQuery.getQuery(), granularity, granularityRange, intervalEndTime, userQuery.getTimeseriesRange());
            JobMetadata job = new JobMetadata(userQuery, query);
            job.setFrequency(granularity.toString());
            job.setEffectiveQueryTime(intervalEndTime);
            // set egads config
            EgadsConfig config;
            config = EgadsConfig.fromProperties(EgadsConfig.fromFile());
            config.setTsModel(userQuery.getTsModels());
            config.setAdModel(userQuery.getAdModels());
            // detect anomalies
            List<EgadsResult> egadsResult = serviceFactory.newDetectorServiceInstance().detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    userQuery.getDetectionWindow(),
                    config
            );
            // results
            List<Anomaly> anomalies = new ArrayList<>();
            List<ImmutablePair<Integer, String>> timeseriesNames = new ArrayList<>();
            int i = 0;
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
                timeseriesNames.add(new ImmutablePair<>(i++, result.getBaseName()));
            }
            List<AnomalyReport> reports = serviceFactory.newJobExecutionService().getReports(anomalies, job);
            tableParams.put(Constants.INSTANTVIEW, "true");
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            instantReportParams.put("tableHtml", thymeleaf.render(new ModelAndView(tableParams, "table")));
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            instantReportParams.put("data", new Gson().toJson(EgadsResult.fuseResults(egadsResult), jsonType));
            instantReportParams.put("timeseriesNames", timeseriesNames);
            instantReportParams.put("userQuery", userQuery);
            instantReportParams.put(Constants.JOB_ID, null);
        } catch (IOException | ClusterNotFoundException | DruidException | SherlockException e) {
            log.error("Error while processing instant job!", e);
            params.put(Constants.ERROR, e.toString());
            return e.getMessage();
        } catch (Exception e) {
            log.error("Unexpected error!", e);
            params.put(Constants.ERROR, e.toString());
            return e.getMessage();
        }
        return Constants.SUCCESS;
    }

    /**
     * Show instant anomaly report.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return render the report
     */
    public static ModelAndView getInstantAnomalyJob(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        if (instantReportParams.containsKey("data")) {
            params.putAll(instantReportParams);
            return new ModelAndView(params, "reportInstant");
        } else {
            params.put(Constants.ERROR, "Not Found!");
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        return null;
    }

    /**
     * Show instant anomaly report.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return render the report
     */
    public static ModelAndView getChart(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        try {
            UserQuery userQuery = new Gson().fromJson("{}", UserQuery.class);
            Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
            Integer start = NumberUtils.parseInt(request.params(Constants.START_DATE));
            String selectedSeries = request.params(Constants.SELECTED_SERIES);
            JobMetadata job = jobAccessor.getJobMetadata(jobId.toString());
            Integer end = start + (Granularity.getValue(job.getGranularity()).getMinutes() * 2);
            Integer detectionWindow = 5;
            if (request.params(Constants.DETECTION_WINDOW) != null) {
                detectionWindow = NumberUtils.parseInt(request.params(Constants.DETECTION_WINDOW));
            }
            log.info("detection window set to {}", detectionWindow);
            params.put("job", job.toString());
            params.put("selectedSeries", selectedSeries);
            params.put("detectionWindow", detectionWindow);
            Granularity granularity = Granularity.getValue(job.getGranularity());
            Integer granularityRange = job.getGranularityRange();
            Query query = serviceFactory.newDruidQueryServiceInstance().build(job.getQuery(), granularity, granularityRange, end, job.getTimeseriesRange());
            job.setFrequency(granularity.toString());
            job.setEffectiveQueryTime(end);
            // set egads config
            EgadsConfig config;
            config = EgadsConfig.fromProperties(EgadsConfig.fromFile());
            config.setTsModel(job.getTimeseriesModel());
            config.setAdModel(job.getAnomalyDetectionModel());
            // detect anomalies
            List<EgadsResult> egadsResult = serviceFactory.newDetectorServiceInstance().detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    detectionWindow,
                    config
            );
            // results
            List<Anomaly> anomalies = new ArrayList<>();
            List<ImmutablePair<Integer, String>> timeseriesNames = new ArrayList<>();
            int i = 0;
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
                timeseriesNames.add(new ImmutablePair<>(i++, result.getBaseName()));
            }
            List<AnomalyReport> reports = serviceFactory.newJobExecutionService().getReports(anomalies, job);
            tableParams.put(Constants.INSTANTVIEW, "true");
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            tableParams.put(Constants.SELECTED_DATE, end);
            tableParams.put(Constants.DETECTION_WINDOW, detectionWindow.toString());
            tableParams.put(Constants.HTTP_BASE_URI, CLISettings.HTTP_BASE_URI);
            params.put("tableHtml", thymeleaf.render(new ModelAndView(tableParams, "table")));
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            params.put(DatabaseConstants.ANOMALIES, reports);
            params.put("data", new Gson().toJson(EgadsResult.fuseResults(egadsResult), jsonType));
            params.put("timeseriesNames", timeseriesNames);
            params.put("userQuery", userQuery);
            params.put(Constants.JOB_ID, jobId);
            return new ModelAndView(params, "reportInstant");
        } catch (IOException | ClusterNotFoundException | DruidException | SherlockException e) {
            log.error("Error while processing instant job!", e);
        } catch (Exception e) {
            log.error("Unexpected error!", e);
        }
        return new ModelAndView(params, "404");
    }

    /**
     * Method for saving user anomaly job into database.
     *
     * @param request  HTTP request whose body contains the job info
     * @param response HTTP response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String saveUserJob(Request request, Response response) {
        log.info("Getting user query from request to save it in database.");
        try {
            // Parse user request
            UserQuery userQuery = new Gson().fromJson(request.body(), UserQuery.class);
            // Validate user email
            EmailService emailService = serviceFactory.newEmailServiceInstance();
            if (!validEmail(userQuery.getOwnerEmail())) {
                throw new SherlockException("Invalid owner email passed");
            }
            log.info("User request parsing successful.");
            DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();
            Query query = queryService.build(userQuery.getQuery(), Granularity.getValue(userQuery.getGranularity()), userQuery.getGranularityRange(), null, userQuery.getTimeseriesRange());
            log.info("Query generation successful.");
            // Create and store job metadata
            JobMetadata jobMetadata = new JobMetadata(userQuery, query);
            jobAccessor.putJobMetadata(jobMetadata);
            response.status(200);
            return jobMetadata.getJobId().toString(); // return job ID
        } catch (Exception e) {
            response.status(500);
            log.error("Error ocurred while saving job!", e);
            return e.getMessage();
        }
    }

    /**
     * Method for deleting user anomaly job into database.
     *
     * @param request  Request for an anomaly report
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String deleteJob(Request request, Response response) {
        Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
        if (jobId == null) {
            response.status(500);
            return "Invalid Job!";
        }
        log.info("Getting job list from database to delete the job [{}]", jobId);
        try {
            schedulerService.stopJob(jobId);
            jobAccessor.deleteJobMetadata(jobId);
            return Constants.SUCCESS;
        } catch (IOException | JobNotFoundException | SchedulerException e) {
            response.status(500);
            log.error("Error in delete job metadata!", e);
            return e.getMessage();
        }
    }

    /**
     * Method for deleting user selected anomaly jobs into database.
     *
     * @param request  Request for an anomaly report
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String deleteSelectedJobs(Request request, Response response) {
        Set<String> jobIds = NumberUtils.convertValidIntNumbers(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER));
        if (jobIds == null || jobIds.size() == 0) {
            response.status(500);
            return "Invalid Jobs!";
        }
        log.info("Getting job list from database to delete the job [{}]", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        try {
            schedulerService.stopJob(jobIds);
            jobAccessor.deleteJobs(jobIds);
            return Constants.SUCCESS;
        } catch (IOException | SchedulerException e) {
            response.status(500);
            log.error("Error in delete job metadata!", e);
            return e.getMessage();
        }
    }

    /**
     * Method called upon request for active jobs list.
     *
     * @param request  Request for jobs list
     * @param response Response
     * @return template view route
     */
    public static ModelAndView viewJobsList(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting job list from database");
            params.put("jobs", jobAccessor.getJobMetadataList());
            params.put("timelineData", jobTimeline.getCurrentTimelineJson(Granularity.HOUR));
            params.put(Constants.TITLE, "Active Jobs");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting job lists!", e);
        }
        return new ModelAndView(params, "listJobs");
    }

    /**
     * Method called upon request for deleted jobs list.
     *
     * @param request  Request for jobs list
     * @param response Response
     * @return template view route
     */
    public static ModelAndView viewDeletedJobsList(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting deleted job list from database.");
            params.put("jobs", deletedJobAccessor.getDeletedJobMetadataList());
            params.put(Constants.TITLE, "Deleted Jobs");
            params.put(Constants.DELETEDJOBSVIEW, "true");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting deleted job lists!", e);
        }
        return new ModelAndView(params, "listJobs");
    }

    /**
     * Method called upon request for a job info page.
     *
     * @param request  Request for job info page
     * @param response Response
     * @return template view route
     * @throws IOException IO exception
     */
    public static ModelAndView viewJobInfo(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting job from database.");
            JobMetadata job = jobAccessor.getJobMetadata(request.params(Constants.ID));
            List<DruidCluster> druidClusters = clusterAccessor.getDruidClusterList();
            params.put("job", job);
            params.put(Constants.DRUID_CLUSTERS, druidClusters);
            params.put(Constants.TITLE, "Job Details");
            params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
            params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
            params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
            params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
            params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
            params.put(Constants.MINUTE, Constants.MAX_MINUTE);
            params.put(Constants.HOUR, Constants.MAX_HOUR);
            params.put(Constants.DAY, Constants.MAX_DAY);
            params.put(Constants.WEEK, Constants.MAX_WEEK);
            params.put(Constants.MONTH, Constants.MAX_MONTH);
            params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
            params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
            boolean isClusterPresent = druidClusters.stream().anyMatch(c -> c.getClusterId().equals(job.getClusterId()));
            params.put(Constants.IS_CLUSTER_PRESENT, isClusterPresent);
            if (!isClusterPresent) {
                log.warn("Cluster ID {} not present for job ID {}", job.getClusterId(), job.getJobId());
                params.put(Constants.ERROR, "No associated druid cluster for this Job! Please select one below");
            }
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting job info!", e);
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        return new ModelAndView(params, "jobInfo");
    }

    /**
     * Method called upon request for a deleted job info page.
     *
     * @param request  Request for job info page
     * @param response Response
     * @return template view route
     * @throws IOException IO exception
     */
    public static ModelAndView viewDeletedJobInfo(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting deleted job Info from database.");
            JobMetadata job = deletedJobAccessor.getDeletedJobMetadata(request.params(Constants.ID));
            params.put("job", job);
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
            params.put(Constants.TITLE, "Deleted Job Details");
            params.put(Constants.DELETEDJOBSVIEW, "true");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting deleted job info!", e);
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        return new ModelAndView(params, "jobInfo");
    }

    /**
     * Helper method to validate input email-ids from user.
     * @return true if email input field is valid else false
     */
    private static boolean validEmail(String emails) {
        return emails == null || emails.isEmpty() || EmailService.validateEmail(emails, EmailService.getValidDomainsFromSettings());
    }

    /**
     * Method for updating anomaly job info into database.
     *
     * @param request  Request for updating a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String updateJobInfo(Request request, Response response) throws IOException {
        Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
        if (jobId == null) {
            response.status(500);
            return "Invalid Job!";
        }
        log.info("Updating job metadata [{}]", jobId);
        try {
            // Parse user request and get existing job
            UserQuery userQuery = new Gson().fromJson(request.body(), UserQuery.class);
            if (!validEmail(userQuery.getOwnerEmail())) {
                throw new SherlockException("Invalid owner email passed");
            }
            JobMetadata currentJob = jobAccessor.getJobMetadata(jobId.toString());
            // Validate query change if any
            Query query = null;
            String newQuery = userQuery.getQuery().replaceAll(Constants.WHITESPACE_REGEX, "");
            String oldQuery = currentJob.getUserQuery().replaceAll(Constants.WHITESPACE_REGEX, "");
            if (!oldQuery.equals(newQuery)) {
                log.info("Validating altered user query");
                DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();
                query = queryService.build(userQuery.getQuery(), Granularity.getValue(userQuery.getGranularity()), userQuery.getGranularityRange(), null, userQuery.getTimeseriesRange());
            }
            JobMetadata updatedJob = new JobMetadata(userQuery, query);
            boolean isRerunRequired = (currentJob.isScheduleChangeRequire(userQuery) || query != null) && currentJob.isRunning();
            currentJob.update(updatedJob);
            // reschedule if needed and store in the database
            if (isRerunRequired) {
                log.info("Scheduling the job with new granularity and/or new frequency and/or new query and/or new cluster/schedule-time.");
                schedulerService.stopJob(currentJob.getJobId());
                schedulerService.scheduleJob(currentJob);
            }
            jobAccessor.putJobMetadata(currentJob);
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while updating the job Info!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method for launching anomaly job.
     *
     * @param request  Request for launching a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String launchJob(Request request, Response response) throws IOException {
        Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
        if (jobId == null) {
            response.status(500);
            return "Invalid Job!";
        }
        log.info("Launching the job requested by user.");
        JobMetadata jobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(jobId.toString());
            DruidCluster cluster = clusterAccessor.getDruidCluster(jobMetadata.getClusterId());
            jobMetadata.setHoursOfLag(cluster.getHoursOfLag());
            log.info("Scheduling job {}", jobId);
            // schedule the job
            schedulerService.scheduleJob(jobMetadata);
            // change the job status as running
            jobMetadata.setJobStatus(JobStatus.RUNNING.getValue());
            jobAccessor.putJobMetadata(jobMetadata);
            response.status(200);
            // return
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while launching the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method for launching selected anomaly jobs.
     *
     * @param request  Request for launching selected jobs
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String launchSelectedJobs(Request request, Response response) throws IOException {
        Set<String> jobIds = NumberUtils.convertValidIntNumbers(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER));
        if (jobIds == null || jobIds.size() == 0) {
            response.status(500);
            return "Invalid Jobs!";
        }
        log.info("Launching the jobs id:[{}] requested by user", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        JobMetadata jobMetadata;
        for (String id : jobIds) {
            try {
                // get jobinfo from database
                jobMetadata = jobAccessor.getJobMetadata(id);
                if (jobMetadata.isRunning()) {
                    continue;
                }
                DruidCluster cluster = clusterAccessor.getDruidCluster(jobMetadata.getClusterId());
                jobMetadata.setHoursOfLag(cluster.getHoursOfLag());
                log.info("Scheduling job.");
                // schedule the job
                schedulerService.scheduleJob(jobMetadata);
                // change the job status as running
                jobMetadata.setJobStatus(JobStatus.RUNNING.getValue());
                jobAccessor.putJobMetadata(jobMetadata);
            } catch (Exception e) {
                log.error("Exception while launching the jobs {}!", id, e);
                response.status(500);
                return e.getMessage();
            }
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Method for stopping anomaly job.
     *
     * @param request  Request for stopping a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String stopJob(Request request, Response response) throws IOException {
        Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
        if (jobId == null) {
            response.status(500);
            return "Invalid Job!";
        }
        log.info("Stopping the job requested by user.");
        JobMetadata jobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(request.params(Constants.ID));
            // stop the job in the sheduler
            schedulerService.stopJob(jobMetadata.getJobId());
            // change the job status to stopped
            jobMetadata.setJobStatus(JobStatus.STOPPED.getValue());
            jobAccessor.putJobMetadata(jobMetadata);
            response.status(200);
            // return
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while stopping the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method for stopping selected anomaly jobs.
     *
     * @param request  Request for stopping selected jobs
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String stopSelectedJobs(Request request, Response response) throws IOException {
        Set<String> jobIds = NumberUtils.convertValidIntNumbers(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER));
        if (jobIds == null || jobIds.size() == 0) {
            response.status(500);
            return "Invalid Jobs!";
        }
        log.info("Stopping the jobs id:[{}] requested by user", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        JobMetadata jobMetadata;
        for (String id : jobIds) {
            try {
                // get jobinfo from database
                jobMetadata = jobAccessor.getJobMetadata(id);
                // stop the job in the sheduler
                schedulerService.stopJob(jobMetadata.getJobId());
                // change the job status to stopped
                jobMetadata.setJobStatus(JobStatus.STOPPED.getValue());
                jobAccessor.putJobMetadata(jobMetadata);
            } catch (Exception e) {
                log.error("Exception while stopping the job {}!", id, e);
                response.status(500);
                return e.getMessage();
            }
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Method for cloning anomaly job.
     *
     * @param request  Request for cloning a job
     * @param response Response
     * @return cloned jobId
     * @throws IOException IO exception
     */
    public static String cloneJob(Request request, Response response) throws IOException {
        Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
        if (jobId == null) {
            response.status(500);
            return "Invalid Job!";
        }
        log.info("Cloning the job...");
        JobMetadata jobMetadata;
        JobMetadata clonedJobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(jobId.toString());
            // copy the job metadata
            clonedJobMetadata = JobMetadata.copyJob(jobMetadata);
            clonedJobMetadata.setJobStatus(JobStatus.CREATED.getValue());
            clonedJobMetadata.setTestName(clonedJobMetadata.getTestName() + Constants.CLONED);
            clonedJobMetadata.setJobId(null);
            String clonnedJobId = jobAccessor.putJobMetadata(clonedJobMetadata);
            response.status(200);
            return clonnedJobId;
        } catch (Exception e) {
            log.error("Exception while cloning the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Rerun the job for given timestamp.
     * Request consist of job id and timestamp
     * @param request HTTP request
     * @param response HTTP response
     * @return status of request
     */
    public static String rerunJob(Request request, Response response) {
        try {
            Map<String, String> params = new Gson().fromJson(
                request.body(),
                new TypeToken<Map<String, String>>() { }.getType()
            );
            Integer jobId = NumberUtils.parseInt(params.get("jobId"));
            Long start = NumberUtils.parseLong(params.get("timestamp"));
            if (jobId == null || start == null) {
                response.status(500);
                return "Invalid Job!";
            }
            JobMetadata job = jobAccessor.getJobMetadata(jobId.toString());
            long end = start + Granularity.getValue(job.getGranularity()).getMinutes();
            serviceFactory.newJobExecutionService().performBackfillJob(job, TimeUtils.zonedDateTimeFromMinutes(start), TimeUtils.zonedDateTimeFromMinutes(end));
            response.status(200);
            return Constants.SUCCESS;
        } catch (SherlockException | IOException | JobNotFoundException e) {
            log.error("Error occurred during job backfill!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method to view cron job reports.
     *
     * @param request  User request to view report
     * @param response Response
     * @return template view route
     */
    public static ModelAndView viewJobReport(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // Get the job id
        String jobId = request.params(Constants.ID);
        String frequency = request.params(Constants.FREQUENCY_PARAM);
        if (!NumberUtils.isInteger(jobId) || !Triggers.getAllValues().contains(frequency)) {
            params.put(Constants.ERROR, "Invalid Request");
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        // Get the json timeline data from database
        try {
            List<AnomalyReport> report = reportAccessor.getAnomalyReportsForJob(jobId, frequency);
            JsonTimeline jsonTimeline = Utils.getAnomalyReportsAsTimeline(report);
            String jsonTimelinePoints = new Gson().toJson(jsonTimeline.getTimelinePoints());
            JobMetadata jobMetadata = jobAccessor.getJobMetadata(jobId);
            // populate params for visualization
            params.put(Constants.JOB_ID, jobId);
            params.put(Constants.FREQUENCY, frequency);
            params.put(Constants.HOURS_OF_LAG, jobAccessor.getJobMetadata(jobId).getHoursOfLag());
            params.put(Constants.TIMELINE_POINTS, jsonTimelinePoints);
            params.put(Constants.TITLE, jobMetadata.getTestName());
            params.put(Constants.HTTP_BASE_URI, CLISettings.HTTP_BASE_URI);
            params.put(Constants.DETECTION_WINDOW, "5");
        } catch (Exception e) {
            log.error("Error while viewing job report!", e);
            params.put(Constants.ERROR, e.getMessage());
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        return new ModelAndView(params, "report");
    }

    /**
     * Method to send job report as requested by users based on datetime.
     *
     * @param request  Job report request
     * @param response Response
     * @return report HTML as a string to render on UI
     */
    @SuppressWarnings("unchecked")
    public static String sendJobReport(Request request, Response response) {
        log.info("Processing user request for reports.");
        Map<String, Object> params = new HashMap<>();
        try {
            // parse the json response to get requested datetime of the report
            Map<String, String> requestParamsMap = new Gson().fromJson(request.body(), Map.class);
            String selectedDate = requestParamsMap.get(Constants.SELECTED_DATE);
            String frequency = requestParamsMap.get(Constants.FREQUENCY);
            if (!NumberUtils.isNonNegativeLong(selectedDate) || !Triggers.getAllValues().contains(frequency) || !NumberUtils.isInteger(request.params(Constants.ID))) {
                response.status(500);
                return "Invalid Request";
            }
            // get the report from the database
            List<AnomalyReport> reports = reportAccessor.getAnomalyReportsForJobAtTime(
                    request.params(Constants.ID),
                    selectedDate,
                    frequency
            );
            List<AnomalyReport> anomalousReports = new ArrayList<>(reports.size());
            for (AnomalyReport report : reports) {
                if (report.getAnomalyTimestamps() != null && !report.getAnomalyTimestamps().isEmpty()) {
                    anomalousReports.add(report);
                }
            }
            params.put(DatabaseConstants.ANOMALIES, anomalousReports);
            params.put(Constants.SELECTED_DATE, selectedDate);
            params.put(Constants.HTTP_BASE_URI, CLISettings.HTTP_BASE_URI);
            // render the table HTML of the report
            String tableHtml = thymeleaf.render(new ModelAndView(params, "table"));
            response.status(200);
            // return table HTML as a string
            return tableHtml;
        } catch (Exception e) {
            log.error("Exception while preparing reports to send!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * This route returns the add new Druid cluster form.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return the Druid cluster form
     */
    public static ModelAndView viewNewDruidClusterForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        return new ModelAndView(params, "druidForm");
    }

    /**
     * This route adds a new Druid cluster to the database.
     *
     * @param request  the HTTP request containing the cluster parameters
     * @param response HTTP response
     * @return the new cluster ID
     */
    public static String addNewDruidCluster(Request request, Response response) {
        log.info("Adding a new Druid cluster");
        DruidCluster cluster;
        try {
            // Parse druid cluster params
            String requestJson = request.body();
            log.debug("Received Druid cluster params: {}", requestJson);
            cluster = new Gson().fromJson(requestJson, DruidCluster.class);
            cluster.validate(); // validate parameters
            cluster.setClusterId(null);
            // Store in database
            clusterAccessor.putDruidCluster(cluster);
            response.status(200);
            return cluster.getClusterId().toString();
        } catch (SherlockException | IOException e) {
            log.error("Error occured while adding Druid Cluster!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method returns a populated table of the current Druid clusters.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return list view of Druid clusters
     */
    public static ModelAndView viewDruidClusterList(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting list of Druid clusters from database");
            params.put("clusters", clusterAccessor.getDruidClusterList());
            params.put(Constants.TITLE, "Druid Clusters");
        } catch (Exception e) {
            log.error("Fatal error occured while retrieving Druid clusters!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "druidList");
    }

    /**
     * Method returns a view of a specified Druid cluster's info.
     *
     * @param request  HTTP request containing the cluster ID
     * @param response HTTP response
     * @return view of the Druid cluster's info
     */
    public static ModelAndView viewDruidCluster(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Druid Cluster Details");
        try {
            params.put("cluster", clusterAccessor.getDruidCluster(request.params(Constants.ID)));
            log.info("Cluster retrieved successfully");
        } catch (Exception e) {
            log.error("Fatal error while retrieving cluster!", e);
            params.put(Constants.ERROR, e.getMessage());
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        return new ModelAndView(params, "druidInfo");
    }

    /**
     * Exposed method to delete a Druid cluster with a specified ID.
     *
     * @param request  HTTP request containing the cluster ID param
     * @param response HTTP response
     * @return success if the cluster was deleted or else an error message
     */
    public static String deleteDruidCluster(Request request, Response response) {
        Integer clusterId = NumberUtils.parseInt(request.params(Constants.ID));
        if (clusterId == null) {
            response.status(500);
            return "Invalid Cluster!";
        }
        log.info("Deleting cluster with ID {}", clusterId);
        try {
            List<JobMetadata> associatedJobs = jobAccessor.getJobsAssociatedWithCluster(clusterId.toString());
            if (associatedJobs.size() > 0) {
                log.info("Attempting to delete a cluster that has {} associated jobs", associatedJobs.size());
                response.status(400);
                return String.format("Cannot delete cluster with %d associated jobs", associatedJobs.size());
            }
            clusterAccessor.deleteDruidCluster(clusterId.toString());
            response.status(200);
            return Constants.SUCCESS;
        } catch (IOException | ClusterNotFoundException e) {
            log.error("Error while deleting cluster!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Update a Druid cluster with a specified ID and new parameters.
     *
     * @param request  HTTP request containing the cluster ID and new cluster parameters
     * @param response HTTP response
     * @return 'success' or an error message
     */
    public static String updateDruidCluster(Request request, Response response) {
        Integer clusterId = NumberUtils.parseInt(request.params(Constants.ID));
        if (clusterId == null) {
            response.status(500);
            return "Invalid Cluster!";
        }
        log.info("Updating cluster with ID {}", clusterId);
        DruidCluster existingCluster;
        DruidCluster updatedCluster;
        try {
            existingCluster = clusterAccessor.getDruidCluster(clusterId.toString());
            updatedCluster = new Gson().fromJson(request.body(), DruidCluster.class);
            updatedCluster.validate();
            boolean requireReschedule = !existingCluster.getHoursOfLag().equals(updatedCluster.getHoursOfLag());
            existingCluster.update(updatedCluster);
            // Put updated cluster in DB
            clusterAccessor.putDruidCluster(existingCluster);
            if (requireReschedule) {
                log.info("Hours of lag has changed, rescheduling jobs for cluster");
                List<JobMetadata> jobs = jobAccessor.getJobsAssociatedWithCluster(clusterId.toString());
                for (JobMetadata job : jobs) {
                    job.setHoursOfLag(existingCluster.getHoursOfLag());
                }
                jobs.removeIf(jobMetadata -> !jobMetadata.isRunning());
                schedulerService.stopAndReschedule(jobs);
                jobAccessor.putJobMetadata(jobs);
            }
            log.info("Druid cluster updated successfully");
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Fatal error while updating Druid cluster!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Retrieve jobs related to given druid cluster update.
     *
     * @param request  HTTP request containing the cluster ID and new cluster parameters
     * @param response HTTP response
     * @return 'success' or an error message
     */
    public static String affectedJobs(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        Integer clusterId = NumberUtils.parseInt(request.params(Constants.ID));
        if (clusterId == null) {
            response.status(500);
            return "Invalid Cluster!";
        }
        log.info("Retrieving jobs for given cluster id {}", clusterId);
        try {
            DruidCluster druidCluster = clusterAccessor.getDruidCluster(clusterId.toString());
            List<JobMetadata> jobs = jobAccessor.getJobsAssociatedWithCluster(druidCluster.getClusterId().toString());
            jobs.removeIf(jobMetadata -> !(druidCluster.getHoursOfLag().equals(jobMetadata.getHoursOfLag()) && jobMetadata.isRunning()));
            params.put("jobs", jobs);
            String listHtml = thymeleaf.render(new ModelAndView(params, "listTemplate"));
            response.status(200);
            return listHtml;
        } catch (Exception e) {
            log.error("Fatal error while retrieving affected jobs for cluster SLA update!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * This method will get the entire backend database as a JSON
     * string and return it to the caller.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return the backend database as a JSON string dump
     */
    public static String getDatabaseJsonDump(Request request, Response response) {
        try {
            JsonObject obj = jsonDumper.getRawData();
            return new Gson().toJson(obj);
        } catch (Exception e) {
            log.error("Error while getting backend json dump!", e);
            return e.getMessage();
        }
    }

    /**
     * A post request to this method where the request body
     * is the JSON string that specifies what to add
     * to the backend database using the JSON dumper.
     *
     * @param request  HTTP request whose body is the JSON
     * @param response HTTP response
     * @return 'OK' if the write is successful, else an error message
     */
    public static String writeDatabaseJsonDump(Request request, Response response) {
        String json = request.body();
        try {
            JsonParser parser = new JsonParser();
            JsonObject obj = parser.parse(json).getAsJsonObject();
            jsonDumper.writeRawData(obj);
            response.status(200);
            return "OK";
        } catch (Exception e) {
            response.status(500);
            log.error("Error while writing JSON to backend!", e);
            return e.getMessage();
        }
    }

    /**
     * Method to view meta settings page.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return object of ModelAndView
     */
    public static ModelAndView viewSettings(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Meta Manager");
        try {
            params.put("jobs", jobAccessor.getJobMetadataList());
            params.put("queuedJobs", jsonDumper.getQueuedJobs());
            params.put("emails", emailMetadataAccessor.getAllEmailMetadata());
        } catch (Exception e) {
            log.error("Fatal error while retrieving settings!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "settings");
    }

    /**
     * Method to view metadata about selected email.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return object of ModelAndView
     */
    public static ModelAndView viewEmails(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Email Settings");
        try {
            List<String> triggers = Triggers.getAllValues();
            triggers.remove(Triggers.MINUTE.toString());
            params.put("emailTriggers", triggers);
            params.put("email", emailMetadataAccessor.getEmailMetadata(request.params(Constants.ID)));
        } catch (Exception e) {
            log.error("Fatal error while retrieving email settings!", e);
            params.put(Constants.ERROR, e.getMessage());
            halt(404, thymeleaf.render(new ModelAndView(params, "404")));
        }
        return new ModelAndView(params, "emailInfo");
    }

    /**
     * Method to update email metadata as requested.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return emailid as string
     */
    public static String updateEmails(Request request, Response response) {
        log.info("Updating email metadata");
        try {
            EmailMetaData newEmailMetaData = new Gson().fromJson(request.body(), EmailMetaData.class);
            if (!EmailService.validateEmail(newEmailMetaData.getEmailId(), EmailService.getValidDomainsFromSettings())) {
                response.status(500);
                return "Invalid Email!";
            }
            EmailMetaData oldEmailMetadata = emailMetadataAccessor.getEmailMetadata(newEmailMetaData.getEmailId());
            if (!newEmailMetaData.getRepeatInterval().equalsIgnoreCase(oldEmailMetadata.getRepeatInterval())) {
                emailMetadataAccessor.removeFromTriggerIndex(newEmailMetaData.getEmailId(), oldEmailMetadata.getRepeatInterval());
            }
            emailMetadataAccessor.putEmailMetadata(newEmailMetaData);
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while stopping the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method to delete email metadata as requested.
     * @param request  HTTP request
     * @param response HTTP response
     * @return status string
     */
    public static String deleteEmail(Request request, Response response) {
        log.info("Deleting email metadata");
        try {
            EmailMetaData emailMetaData = new Gson().fromJson(request.body(), EmailMetaData.class);
            if (!EmailService.validateEmail(emailMetaData.getEmailId(), EmailService.getValidDomainsFromSettings())) {
                response.status(500);
                return "Invalid Email!";
            }
            EmailMetaData emailMetadata = emailMetadataAccessor.getEmailMetadata(emailMetaData.getEmailId());
            jobAccessor.deleteEmailFromJobs(emailMetadata);
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while stopping the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Shows an advanced instant query view.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return query form
     * @throws IOException if an error occurs while getting the cluster list
     */
    public static ModelAndView debugInstantReport(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        return new ModelAndView(params, "debugForm");
    }

    /**
     * Processe a power query and store the results.
     * Send the job and query ID back to the UI.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return `[queryId]-[jobId]`
     */
    public static ModelAndView debugPowerQuery(Request request, Response response) {
        Map<String, Object> modelParams = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        modelParams.put(Constants.TITLE, "Power Query");
        try {
            Map<String, String> params = Utils.queryParamsToStringMap(request.queryMap());
            Query query = QueryBuilder.start()
                    .endAt(params.get("endTime"))
                    .startAt(params.get("startTime"))
                    .queryString(params.get("query"))
                    .granularity(params.get("granularity"))
                    .intervals(params.get("intervals"))
                    .build();
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            JobMetadata job = new JobMetadata(userQuery, query);
            JobExecutionService executionService = serviceFactory.newJobExecutionService();
            DruidCluster cluster = clusterAccessor.getDruidCluster(job.getClusterId());
            List<Anomaly> anomalies = executionService.executeJob(job, cluster, query);
            List<AnomalyReport> reports = executionService.getReports(anomalies, job);
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            String tableHtml = thymeleaf.render(new ModelAndView(tableParams, "table"));
            modelParams.put("tableHtml", tableHtml);
        } catch (Exception e) {
            log.error("Exception", e);
            modelParams.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(modelParams, "reportInstant");
    }

    /**
     * Send the form used to perform backfill jobs.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return backfill form view
     * @throws IOException if there was an error getting the job list
     */
    public static ModelAndView debugBackfillForm(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put("jobs", jobAccessor.getJobMetadataList());
        return new ModelAndView(params, "debugBackfill");
    }

    /**
     * Process and run a backfill job.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return an error message or 'success'
     */
    public static String debugRunBackfillJob(Request request, Response response) {
        try {
            Map<String, String> params = new Gson().fromJson(
                    request.body(),
                    new TypeToken<Map<String, String>>() { }.getType()
            );
            String[] jobIds = params.get("jobId").split(Constants.COMMA_DELIMITER);
            ZonedDateTime startTime = TimeUtils.parseDateTime(params.get("fillStartTime"));
            ZonedDateTime endTime = ("".equals(params.get("fillEndTime")) || params.get("fillEndTime") == null) ? null : TimeUtils.parseDateTime(params.get("fillEndTime"));
            for (String jobId : jobIds) {
                JobMetadata job = jobAccessor.getJobMetadata(jobId);
                serviceFactory.newJobExecutionService().performBackfillJob(job, startTime, endTime);
            }
            response.status(200);
            return "Success";
        } catch (SherlockException | IOException | JobNotFoundException e) {
            log.error("Error occurred during job backfill!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Endpoint used to clear all reports associated with a job.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return 'success' or an error message is something goes wrong
     */
    public static String debugClearJobReports(Request request, Response response) {
        try {
            reportAccessor.deleteAnomalyReportsForJob(request.params(Constants.ID));
            return "Success";
        } catch (Exception e) {
            log.error("Error clearing job reports!", e);
            return e.getMessage();
        }
    }

    /**
     * Endpoint used to clear all reports associated with given jobs.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return 'success' or an error message is something goes wrong
     */
    public static String clearReportsOfSelectedJobs(Request request, Response response) {
        Set<String> jobIds = NumberUtils.convertValidIntNumbers(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER));
        if (jobIds == null || jobIds.size() == 0) {
            response.status(500);
            return "Invalid Jobs!";
        }
        log.info("Clearing reports of jobs id:[{}] requested by user", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        for (String id : jobIds) {
            try {
                reportAccessor.deleteAnomalyReportsForJob(id);
            } catch (Exception e) {
                log.error("Error clearing reports of the jobs {}!", id, e);
                response.status(500);
                return e.getMessage();
            }
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Endpoint used to clear jobs created from the debug interface.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return 'success' or an error message
     */
    public static String debugClearDebugJobs(Request request, Response response) {
        try {
            jobAccessor.deleteDebugJobs();
            return "Success";
        } catch (IOException e) {
            return e.getMessage();
        }
    }

    /**
     * Display a query page with EGADS configurable params.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return query page
     * @throws IOException if an array occurs while getting the clusters
     */
    public static ModelAndView debugShowEgadsConfigurableQuery(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        params.put("filteringMethods", EgadsConfig.FilteringMethod.values());
        return new ModelAndView(params, "debugMegaQuery");
    }

    /**
     * Perform an EGADS-configured query. This method will get the
     * EGADS configuration parameters from the request body
     * and build an {@code EgadsConfig} object.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return the query ID and job ID
     */
    public static ModelAndView debugPerformEgadsQuery(Request request, Response response) {
        Map<String, Object> modelParams = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        modelParams.put(Constants.TITLE, "Egads Query");
        try {
            Map<String, String> params = Utils.queryParamsToStringMap(request.queryMap());
            Query query = QueryBuilder.start()
                    .endAt(params.get("endTime"))
                    .startAt(params.get("startTime"))
                    .queryString(params.get("query"))
                    .granularity(params.get("granularity"))
                    .intervals(params.get("intervals"))
                    .build();
            EgadsConfig egadsConfig = EgadsConfig.create()
                    .maxAnomalyTimeAgo(params.get("maxAnomalyTimeAgo"))
                    .aggregation(params.get("aggregation"))
                    .timeShifts(params.get("timeShifts"))
                    .baseWindows(params.get("baseWindows"))
                    .period(params.get("period"))
                    .fillMissing(params.get("fillMissing"))
                    .numWeeks(params.get("numWeeks"))
                    .numToDrop(params.get("numToDrop"))
                    .dynamicParameters(params.get("dynamicParameters"))
                    .autoAnomalyPercent(params.get("autoSensitivityAnomalyPercent"))
                    .autoStandardDeviation(params.get("autoSensitivityStandardDeviation"))
                    .preWindowSize(params.get("preWindowSize"))
                    .postWindowSize(params.get("postWindowSize"))
                    .confidence(params.get("confidence"))
                    .windowSize(params.get("windowSize"))
                    .filteringMethod(params.get("filteringMethod"))
                    .filteringParam(params.get("filteringParam"))
                    .build();
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            JobMetadata job = new JobMetadata(userQuery, query);
            JobExecutionService executionService = serviceFactory.newJobExecutionService();
            DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
            List<EgadsResult> egadsResult = detectorService.detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    null,
                    egadsConfig
            );
            List<Anomaly> anomalies = new ArrayList<>();
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
            }
            List<AnomalyReport> reports = executionService.getReports(anomalies, job);
            response.status(200);
            Gson gson = new Gson();
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            String data = gson.toJson(EgadsResult.fuseResults(egadsResult), jsonType);
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            String tableHtml = thymeleaf.render(new ModelAndView(tableParams, "table"));
            modelParams.put("tableHtml", tableHtml);
            modelParams.put("data", data);
        } catch (Exception e) {
            log.error("Exception", e);
            modelParams.put(Constants.ERROR, e.toString());
        }
        return new ModelAndView(modelParams, "reportInstant");
    }

    /**
     * Method to view redis restore form.
     * @param request HTTP request
     * @param response HTTP response
     * @return redisRestoreForm html
     * @throws IOException exception
     */
    public static ModelAndView restoreRedisDBForm(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        return new ModelAndView(params, "redisRestoreForm");
    }

    /**
     * Method to process the restore of redis DB from given json file.
     * @param request HTTP request
     * @param response HTTP response
     * @return request status 'success' or error
     */
    public static String restoreRedisDB(Request request, Response response) {
        Map<String, String> params = new Gson().fromJson(request.body(), new TypeToken<Map<String, String>>() { }.getType());
        String filePath = params.get(Constants.PATH);
        try {
            schedulerService.removeAllJobsFromQueue();
        } catch (SchedulerException e) {
            log.error("Error while unscheduling current jobs!", e);
            response.status(500);
            return e.getMessage();
        }
        try {
            jsonDumper.writeRawData(BackupUtils.getDataFromJsonFile(filePath));
        } catch (IOException e) {
            log.error("Unable to load data from the file at {} ", filePath, e);
            response.status(500);
            return e.getMessage();
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Method to build indexes for database.
     * @param request HTTP request
     * @param response HTTP response
     * @return request status 'success' or error
     */
    public static String buildIndexes(Request request, Response response) {
        try {
            Set<String> jobIds = jobAccessor.getJobIds();
            Set<String> clusterIds = clusterAccessor.getDruidClusterIds();
            Set<String> emailIds = emailMetadataAccessor.getAllEmailIds();
            for (String clusterId : clusterIds) {
                jsonDumper.clearIndexes(DatabaseConstants.INDEX_JOB_CLUSTER_ID, clusterId);
            }
            for (JobStatus jobStatus : JobStatus.values()) {
                jsonDumper.clearIndexes(DatabaseConstants.INDEX_JOB_STATUS, jobStatus.getValue());
            }
            for (String emailId : emailIds) {
                jsonDumper.clearIndexes(DatabaseConstants.INDEX_EMAILID_JOBID, emailId);
            }
            for (String clusterId: clusterIds) {
                try {
                    DruidCluster druidCluster = clusterAccessor.getDruidCluster(clusterId);
                    clusterAccessor.putDruidCluster(druidCluster);
                } catch (ClusterNotFoundException e) {
                    log.error("Cluster with Id {} not found! removing from the index", clusterId);
                    clusterAccessor.removeFromClusterIdIndex(clusterId);
                } catch (IOException e) {
                    log.error("Exception while processing jobId {} : {}", clusterId, e.getMessage());
                }
            }
            for (String jobId: jobIds) {
                try {
                    JobMetadata jobMetadata = jobAccessor.getJobMetadata(jobId);
                    jobAccessor.putJobMetadata(jobMetadata);
                } catch (JobNotFoundException e) {
                    log.error("Job with Id {} not found! removing from the index", jobId);
                    jobAccessor.removeFromJobIdIndex(jobId);
                } catch (IOException e) {
                    log.error("Exception while processing jobId {} : {}", jobId, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Error while rebuilding indexes!", e);
            response.status(500);
            return e.getMessage();
        }
        response.status(200);
        return Constants.SUCCESS;
    }
}
