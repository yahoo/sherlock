/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.settings.CLISettings;
import lombok.extern.slf4j.Slf4j;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.redirect;
import static spark.Spark.staticFiles;

/**
 * App main class.
 */
@Slf4j
class App {

    /**
     * Class JCommander instance.
     */
    private static JCommander jCommander = new JCommander();
    /**
     * Class CLISettings instances.
     */
    private static CLISettings settings = new CLISettings();
    /**
     * Class application instance.
     */
    private static App app = new App();

    /**
     * Main() function.
     *
     * @param args commandline arguments
     * @throws IOException if an error occurs parsing the arguments
     *                     or the configuration file.
     */
    public static void main(String[] args) throws IOException, SherlockException {
        // Parse CLI args and store
        jCommander.addObject(settings);
        jCommander.parse(args);
        // Attempt to read settings from a file
        settings.loadFromConfig();
        // check for email service cli args
        if (CLISettings.ENABLE_EMAIL) {
            if (CLISettings.FROM_MAIL == null ||
                CLISettings.REPLY_TO == null ||
                CLISettings.FAILURE_EMAIL == null ||
                CLISettings.SMTP_HOST == null) {
                throw new ParameterException("Please specify all [--from-mail, --reply-to, --failure-email, --smtp-host] if you want to enable email service!");
            }
        }
        // Print the settings
        settings.print();
        // Check if we want to print help
        jCommander.setProgramName("AnomalyDetector");
        if (CLISettings.HELP) {
            jCommander.usage();
            return;
        }
        log.info("Starting the app...");
        app.run();
    }

    /**
     * Run the server.
     */
    public void run() throws SherlockException {
        // Launch the web server
        try {
            startWebServer();
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage());
            throw new SherlockException(e.getMessage(), e);
        }
    }

    /**
     * Call to Routes to initiate accessors and parameters.
     *
     * @throws SherlockException if an error initiating occurs
     */
    public void initRoutes() throws SherlockException {
        Routes.init();
    }

    /**
     * Configure the routing for all pages and start the web server.
     */
    public void startWebServer() throws SherlockException {
        log.info("Running on port: {}", CLISettings.PORT);
        // Set the port
        port(CLISettings.PORT);

        // static files (css, js, img) location
        staticFiles.location("/templates/static");

        // external static files
        if (CLISettings.EXTERNAL_FILE_PATH != null) {
            staticFiles.externalLocation(CLISettings.EXTERNAL_FILE_PATH);
        }

        // Home Page
        get("/sherlock", Routes::viewHomePage, new ThymeleafTemplateEngine());

        // Default redirect
        redirect.get("/", "/sherlock");

        // Route for New job submission form
        get("/New", Routes::viewNewAnomalyJobForm, new ThymeleafTemplateEngine());

        // Route for instant anomaly-detection form
        get("/Flash-Query", Routes::viewInstantAnomalyJobForm, new ThymeleafTemplateEngine());

        // Route for instant anomaly-detection on user input query
        post("/Flash-Query/ProcessAnomalyReport", Routes::processInstantAnomalyJob, new ThymeleafTemplateEngine());

        // Route for viewing deleted jobs
        get("/DeletedJobs", Routes::viewDeletedJobsList, new ThymeleafTemplateEngine());

        // Route for viewing currently active jobs
        get("/Jobs", Routes::viewJobsList, new ThymeleafTemplateEngine());

        // Route for viewing selected job detail-page
        get("/Jobs/:id", Routes::viewJobInfo, new ThymeleafTemplateEngine());

        // Route for saving user job
        post("/SaveJobInfo", Routes::saveUserJob);

        // Route for deleting job
        post("/DeletedJobs/:id", Routes::deleteJob);

        // Route for viewing deleted job
        get("/DeletedJobs/:id", Routes::viewDeletedJobInfo, new ThymeleafTemplateEngine());

        // Route for cloning selected job
        post("/CloneJob/:id", Routes::cloneJob);

        // Route for updating selected job detail
        post("/UpdateJobInfo/:id", Routes::updateJobInfo);

        // Route for launching selected job
        post("/LaunchJob/:id", Routes::launchJob);

        // Route for stopping selected job
        post("/StopJob/:id", Routes::stopJob);

        // Routes to view reports of the selected job
        get("/Reports/:id/:frequency", Routes::viewJobReport, new ThymeleafTemplateEngine());

        // Routes to send reports as HTML to render on UI as requested by users
        post("/Reports/:id/:frequency", Routes::sendJobReport);

        // Routes to show the new Druid Cluster form
        get("/Druid/NewCluster", Routes::viewNewDruidClusterForm, new ThymeleafTemplateEngine());

        // Routes to add a new Druid cluster
        post("/Druid/NewCluster", Routes::addNewDruidCluster);

        // Routes to view the list of Druid clusters
        get("/Druid/Clusters", Routes::viewDruidClusterList, new ThymeleafTemplateEngine());

        // Routes to view a Druid cluster
        get("/Druid/Cluster/:id", Routes::viewDruidCluster, new ThymeleafTemplateEngine());

        // Routes to delete a Druid cluster
        post("/Druid/DeleteCluster/:id", Routes::deleteDruidCluster);

        // Routes to update a Druid cluster
        post("/Druid/UpdateCluster/:id", Routes::updateDruidCluster);

        // Routes to Rerun the job for given timestamp in minutes
        post("/Rerun/:id/:timestamp", Routes::rerunJob);

        // Enable debug routes only in debug mode
        if (CLISettings.DEBUG_MODE) {
            // Routes to get the database as a JSON dump
            get("/DatabaseJson", Routes::getDatabaseJsonDump);
            // Debug job form route
            get("/Debug/InstantReport", Routes::debugInstantReport, new ThymeleafTemplateEngine());
            // Debug job post route
            get("/Debug/ProcessInstantReport", Routes::debugPowerQuery, new ThymeleafTemplateEngine());
            // Debug back fill jobs
            get("/Debug/BackfillReports", Routes::debugBackfillForm, new ThymeleafTemplateEngine());
            // Debug back fill jobs post
            post("/Debug/BackfillReports", Routes::debugRunBackfillJob);
            // Debug remove reports for job
            get("/Debug/DeleteJobReports/:id", Routes::debugClearJobReports);
            // Debug remove debug jobs
            get("/Debug/DeleteDebugJobs", Routes::debugClearDebugJobs);
            // Debug egads configuration query
            get("/Debug/EgadsQuery", Routes::debugShowEgadsConfigurableQuery, new ThymeleafTemplateEngine());
            // Submit egads query
            post("/Debug/EgadsQuery", Routes::debugPerformEgadsQuery);
        }

        initRoutes();

        // Routes configured
        log.info("Routes ready");
        log.info("Site online: http://localhost:{}", CLISettings.PORT);
    }
}
