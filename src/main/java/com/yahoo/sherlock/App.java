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
import com.yahoo.sherlock.settings.Constants;

import org.pac4j.core.authorization.authorizer.csrf.CsrfAuthorizer;
import org.pac4j.core.authorization.authorizer.csrf.CsrfTokenGeneratorAuthorizer;
import org.pac4j.core.authorization.authorizer.csrf.DefaultCsrfTokenGenerator;
import org.pac4j.core.client.Clients;
import org.pac4j.core.client.direct.AnonymousClient;
import org.pac4j.core.config.Config;
import org.pac4j.sparkjava.DefaultHttpActionAdapter;
import org.pac4j.sparkjava.SecurityFilter;

import lombok.extern.slf4j.Slf4j;

import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;

import static spark.Spark.before;
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
     * Thymeleaf template engine object.
     */
    private static ThymeleafTemplateEngine thymeleafTemplateEngine = new ThymeleafTemplateEngine();

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

        // add security/authorization config
        Config config = new Config();
        config.setHttpActionAdapter(new DefaultHttpActionAdapter());
        config.setAuthorizer(new CsrfTokenGeneratorAuthorizer(new DefaultCsrfTokenGenerator()));
        config.setAuthorizer(new CsrfAuthorizer());
        config.setClients(new Clients(AnonymousClient.INSTANCE));

        before("/*", new SecurityFilter(config, null, Constants.CSRF + Constants.COMMA_DELIMITER + Constants.XSS_PROTECTION));

        // Home Page
        get("/sherlock", Routes::viewHomePage, thymeleafTemplateEngine);

        // Default redirect
        redirect.get("/", "/sherlock");

        // Route for New job submission form
        get("/New", Routes::viewNewAnomalyJobForm, thymeleafTemplateEngine);

        // Route for instant anomaly-detection form
        get("/Flash-Query", Routes::viewInstantAnomalyJobForm, thymeleafTemplateEngine);

        // Route for instant anomaly-detection on user input query
        post("/Flash-Query/ProcessAnomalyReport", Routes::processInstantAnomalyJob, thymeleafTemplateEngine);

        // Route for viewing deleted jobs
        get("/DeletedJobs", Routes::viewDeletedJobsList, thymeleafTemplateEngine);

        // Route for viewing currently active jobs
        get("/Jobs", Routes::viewJobsList, thymeleafTemplateEngine);

        // Route for viewing selected job detail-page
        get("/Jobs/:id", Routes::viewJobInfo, thymeleafTemplateEngine);

        // Route for saving user job
        post("/SaveJobInfo", Routes::saveUserJob);

        // Route for deleting job
        post("/DeletedJobs/:id", Routes::deleteJob);

        // Route for viewing deleted job
        get("/DeletedJobs/:id", Routes::viewDeletedJobInfo, thymeleafTemplateEngine);

        // Route for cloning job
        post("/CloneJob/:id", Routes::cloneJob);

        // Route for updating selected job detail
        post("/UpdateJobInfo/:id", Routes::updateJobInfo);

        // Route for launching selected job
        post("/LaunchJob/:id", Routes::launchJob);

        // Route for stopping selected job
        post("/StopJob/:id", Routes::stopJob);

        // Routes to view reports of the selected job
        get("/Reports/:id/:frequency", Routes::viewJobReport, thymeleafTemplateEngine);

        // Routes to send reports as HTML to render on UI as requested by users
        post("/Reports/:id/:frequency", Routes::sendJobReport);

        // Routes to show the new Druid Cluster form
        get("/Druid/NewCluster", Routes::viewNewDruidClusterForm, thymeleafTemplateEngine);

        // Routes to add a new Druid cluster
        post("/Druid/NewCluster", Routes::addNewDruidCluster);

        // Routes to view the list of Druid clusters
        get("/Druid/Clusters", Routes::viewDruidClusterList, thymeleafTemplateEngine);

        // Routes to view a Druid cluster
        get("/Druid/Cluster/:id", Routes::viewDruidCluster, thymeleafTemplateEngine);

        // Routes to delete a Druid cluster
        post("/Druid/DeleteCluster/:id", Routes::deleteDruidCluster);

        // Routes to update a Druid cluster
        post("/Druid/UpdateCluster/:id", Routes::updateDruidCluster);

        // Routes to Rerun the job for given timestamp in minutes
        post("/Rerun", Routes::rerunJob);

        // Routes to meta manager
        get("/Meta-Manager", Routes::viewSettings, thymeleafTemplateEngine);

        // Routes to delete selected jobs
        post("/Meta-Manager/Delete/:ids", Routes::deleteSelectedJobs);

        // Routes to start selected jobs
        post("/Meta-Manager/Launch/:ids", Routes::launchSelectedJobs);

        // Routes to stop selected jobs
        post("/Meta-Manager/Stop/:ids", Routes::stopSelectedJobs);

        // Routes to Clear all reports of selected jobs
        post("/Meta-Manager/ClearReports/:ids", Routes::clearReportsOfSelectedJobs);

        // Routes to view Emails
        get("/Emails/:id", Routes::viewEmails, thymeleafTemplateEngine);

        // Routes to update Emails
        post("/UpdateEmail", Routes::updateEmails);

        // Routes to delete Email
        post("/DeleteEmail", Routes::deleteEmail);

        // Enable debug routes only in debug mode
        if (CLISettings.DEBUG_MODE) {
            // Routes to get the database as a JSON dump
            get("/DatabaseJson", Routes::getDatabaseJsonDump);
            // Debug job form route
            get("/Debug/InstantReport", Routes::debugInstantReport, thymeleafTemplateEngine);
            // Debug job post route
            get("/Debug/ProcessInstantReport", Routes::debugPowerQuery, thymeleafTemplateEngine);
            // Debug back fill jobs
            get("/Debug/BackfillReports", Routes::debugBackfillForm, thymeleafTemplateEngine);
            // Debug back fill jobs post
            post("/Debug/BackfillReports", Routes::debugRunBackfillJob);
            // Debug remove reports for job
            get("/Debug/DeleteJobReports/:id", Routes::debugClearJobReports);
            // Debug remove debug jobs
            get("/Debug/DeleteDebugJobs", Routes::debugClearDebugJobs);
            // Debug egads configuration query
            get("/Debug/EgadsQuery", Routes::debugShowEgadsConfigurableQuery, thymeleafTemplateEngine);
            // Submit egads query
            post("/Debug/EgadsQuery", Routes::debugPerformEgadsQuery);
            // Restore redis db form
            get("/Debug/Restore", Routes::restoreRedisDBForm, thymeleafTemplateEngine);
            // Restore redis db
            post("/Debug/Restore", Routes::restoreRedisDB);
        }

        initRoutes();

        // Routes configured
        log.info("Routes ready");
        log.info("Site online: http://localhost:{}", CLISettings.PORT);
    }
}
