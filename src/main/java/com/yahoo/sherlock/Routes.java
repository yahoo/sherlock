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
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.model.JobTimeline;
import com.yahoo.sherlock.model.JsonTimeline;
import com.yahoo.sherlock.model.UserQuery;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.query.QueryBuilder;
import com.yahoo.sherlock.scheduler.JobExecutionService;
import com.yahoo.sherlock.scheduler.SchedulerService;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.DruidQueryService;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JsonDumper;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.sherlock.utils.Utils;
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

/**
 * Routes logic for web requests.
 */
@Slf4j
public class Routes {

    /**
     * Default parameters.
     */
    public static Map<String, Object> defaultParams;

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
    private static JsonDumper jsonDumper;

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
        defaultParams.put(Constants.FREQUENCIES, Triggers.getAllValues());
        defaultParams.put(Constants.EMAIL_HTML, null);
        defaultParams.put(Constants.EMAIL_ERROR, null);
    }

    /**
     * Initialize and set references to service and
     * accessor objects.
     */
    public static void initServices() {
        thymeleaf = new ThymeleafTemplateEngine();
        serviceFactory = new ServiceFactory();
        schedulerService = serviceFactory.newSchedulerServiceInstance();
        // Grab references to the accessors, initializing them
        reportAccessor = Store.getAnomalyReportAccessor();
        clusterAccessor = Store.getDruidClusterAccessor();
        jobAccessor = Store.getJobMetadataAccessor();
        deletedJobAccessor = Store.getDeletedJobMetadataAccessor();
        jsonDumper = Store.getJsonDumper();
        schedulerService.instantiateMasterScheduler();
        schedulerService.startMasterScheduler();
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
        try {
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        } catch (IOException e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.DRUID_CLUSTERS, new ArrayList<>());
        }
        return new ModelAndView(params, "jobForm");
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
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
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
    public static ModelAndView processInstantAnomalyJob(Request request, Response response) {
        log.info("Getting user query from request.");
        Map<String, Object> params = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Instant Anomaly Report");
        try {
            Map<String, String> paramsMap = Utils.queryParamsToStringMap(request.queryMap());
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            Granularity granularity = Granularity.getValue(paramsMap.get("granularity"));
            Integer hoursOfLag = clusterAccessor.getDruidCluster(paramsMap.get("clusterId")).getHoursOfLag();
            Integer intervalEndTime = granularity.getEndTimeForInterval(
                ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag));
            Query query = serviceFactory.newDruidQueryServiceInstance().build(userQuery.getQuery(), granularity, intervalEndTime);
            JobMetadata job = JobMetadata.fromQuery(userQuery, query);
            job.setEffectiveQueryTime(intervalEndTime);
            List<EgadsResult> egadsResult = serviceFactory.newDetectorServiceInstance().detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    null
            );
            List<Anomaly> anomalies = new ArrayList<>();
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
            }
            List<AnomalyReport> reports = serviceFactory.newJobExecutionService().getReports(anomalies, job);
            tableParams.put(Constants.INSTANTVIEW, "true");
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            params.put("tableHtml", thymeleaf.render(new ModelAndView(tableParams, "table")));
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            params.put("data", new Gson().toJson(EgadsResult.fuseResults(egadsResult), jsonType));
        } catch (IOException | ClusterNotFoundException | DruidException | SherlockException e) {
            log.error("Error while processing instant job!", e);
            params.put(Constants.ERROR, e.toString());
        } catch (Exception e) {
            log.error("Unexpected error!", e);
            params.put(Constants.ERROR, e.toString());
        }
        return new ModelAndView(params, "reportInstant");
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
            if (!emailService.validateEmail(userQuery.getOwnerEmail(), emailService.getValidDomainsFromSettings())) {
                throw new SherlockException("Invalid owner email passed");
            }
            log.info("User request parsing successful.");
            DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();
            Query query = queryService.build(userQuery.getQuery(), Granularity.getValue(userQuery.getGranularity()), null);
            log.info("Query generation successful.");
            // Create and store job metadata
            JobMetadata jobMetadata = JobMetadata.fromQuery(userQuery, query);
            jobMetadata.setHoursOfLag(clusterAccessor.getDruidCluster(jobMetadata.getClusterId()).getHoursOfLag());
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
            params.put("timelineData", new JobTimeline().getCurrentTimelineJson(Granularity.HOUR));
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
            params.put("job", job);
            params.put("clusterName", clusterAccessor.getDruidCluster(job.getClusterId()).getClusterName());
            params.put(Constants.TITLE, "Job Details");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting job info!", e);
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
            params.put("clusterName", clusterAccessor.getDruidCluster(job.getClusterId()).getClusterName());
            params.put(Constants.TITLE, "Deleted Job Details");
            params.put(Constants.DELETEDJOBSVIEW, "true");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting deleted job info!", e);
        }
        return new ModelAndView(params, "jobInfo");
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
        String jobId = request.params(Constants.ID);
        log.info("Updating job metadata [{}]", jobId);
        try {
            // Parse user request and get existing job
            UserQuery userQuery = new Gson().fromJson(request.body(), UserQuery.class);
            // Validate user email
            EmailService emailService = serviceFactory.newEmailServiceInstance();
            if (!emailService.validateEmail(userQuery.getOwnerEmail(), emailService.getValidDomainsFromSettings())) {
                throw new SherlockException("Invalid owner email passed");
            }
            JobMetadata currentJob = jobAccessor.getJobMetadata(jobId);
            Query query = null;
            if (!currentJob.getUserQuery().equals(userQuery.getQuery())) {
                log.info("Validating altered user query");
                DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();
                query = queryService.build(userQuery.getQuery(), Granularity.getValue(userQuery.getGranularity()), null);
            }
            JobMetadata updatedJob = JobMetadata.fromQuery(userQuery, query);
            boolean isRerunRequired = (currentJob.userQueryChangeSchedule(userQuery) || query != null) && currentJob.isRunning();
            currentJob.update(updatedJob);
            // Store in the database and reschedule if needed
            jobAccessor.putJobMetadata(currentJob);
            if (isRerunRequired) {
                log.info("Scheduling the job with new granularity and/or new frequency and/or new query.");
                schedulerService.stopJob(currentJob.getJobId());
                schedulerService.scheduleJob(currentJob);
            }
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
        log.info("Launching the job requested by user.");
        JobMetadata jobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(request.params(Constants.ID));
            DruidCluster cluster = clusterAccessor.getDruidCluster(jobMetadata.getClusterId());
            jobMetadata.setHoursOfLag(cluster.getHoursOfLag());
            log.info("Scheduling job.");
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
     * Method for stopping anomaly job.
     *
     * @param request  Request for stopping a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String stopJob(Request request, Response response) throws IOException {
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
        // Get the json timeline data from database
        try {
            List<AnomalyReport> report = reportAccessor.getAnomalyReportsForJob(jobId, frequency);
            JsonTimeline jsonTimeline = Utils.getAnomalyReportsAsTimeline(report);
            String jsonTimelinePoints = new Gson().toJson(jsonTimeline.getTimelinePoints());
            // populate params for visualization
            params.put(Constants.FREQUENCY, frequency);
            params.put(Constants.HOURS_OF_LAG, jobAccessor.getJobMetadata(jobId).getHoursOfLag());
            params.put(Constants.TIMELINE_POINTS, jsonTimelinePoints);
            params.put(Constants.TITLE, "Anomaly Detection Reports");
        } catch (Exception e) {
            log.error("Error while viewing job report!", e);
            params.put(Constants.ERROR, e.toString());
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
        String clusterId = request.params(Constants.ID);
        log.info("Deleting cluster with ID {}", clusterId);
        try {
            List<JobMetadata> associatedJobs = jobAccessor.getJobsAssociatedWithCluster(clusterId);
            if (associatedJobs.size() > 0) {
                log.info("Attempting to delete a cluster that has {} associated jobs", associatedJobs.size());
                response.status(400);
                return String.format("Cannot delete cluster with %d associated jobs", associatedJobs.size());
            }
            clusterAccessor.deleteDruidCluster(clusterId);
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
        String clusterId = request.params(Constants.ID);
        log.info("Updating cluster with ID {}", clusterId);
        DruidCluster existingCluster;
        DruidCluster updatedCluster;
        try {
            existingCluster = clusterAccessor.getDruidCluster(clusterId);
            updatedCluster = new Gson().fromJson(request.body(), DruidCluster.class);
            updatedCluster.validate();
            boolean requireReschedule = !existingCluster.getHoursOfLag().equals(updatedCluster.getHoursOfLag());
            existingCluster.update(updatedCluster);
            // Put updated cluster in DB
            clusterAccessor.putDruidCluster(existingCluster);
            if (requireReschedule) {
                log.info("Hours of lag has changed, rescheduling jobs for cluster");
                List<JobMetadata> rescheduleJobs = jobAccessor
                        .getRunningJobsAssociatedWithCluster(existingCluster.getClusterId());
                for (JobMetadata job : rescheduleJobs) {
                    job.setHoursOfLag(existingCluster.getHoursOfLag());
                }
                schedulerService.stopAndReschedule(rescheduleJobs);
                jobAccessor.putJobMetadata(rescheduleJobs);
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
            JobMetadata job = JobMetadata.fromQuery(userQuery, query);
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
            JobMetadata job = jobAccessor.getJobMetadata(params.get("jobId"));
            ZonedDateTime startTime = TimeUtils.parseDateTime(params.get("fillStartTime"));
            serviceFactory.newJobExecutionService().performBackfillJob(job, startTime);
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
            JobMetadata job = JobMetadata.fromQuery(userQuery, query);
            JobExecutionService executionService = serviceFactory.newJobExecutionService();
            DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
            List<EgadsResult> egadsResult = detectorService.detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
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

}
