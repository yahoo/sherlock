/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.DetectorResult;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.DetectorConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Service class for anomaly detection.
 */
@Slf4j
public class DetectorService {

    /**
     * Class druid query service instance.
     */
    private DruidQueryService queryService = new DruidQueryService();

    /**
     * Class HTTP service instance.
     */
    private HttpService httpService = new HttpService();

    /**
     * Class time series parser service instance.
     */
    private TimeSeriesParserService parserService = new TimeSeriesParserService();

    /**
     * Egads Detector API Service instance.
     */
    private EgadsAPIService egadsAPIService = new EgadsAPIService();

    /**
     * Prophet Detector API Service instance.
     */
    private ProphetAPIService prophetAPIService = new ProphetAPIService();

    /**
     * Empty constructor.
     */
    public DetectorService() {
    }

    /**
     * Method to detect anomalies.
     * This method handles the control/data flow between components of detection system.
     *
     * @param cluster           the Druid cluster to issue the query
     * @param jobMetadata       job metadata
     * @return list of anomalies
     * @throws SherlockException exeption thrown while runnig the anomaly detector components
     * @throws DruidException    if an error querying druid occurs
     */
    public List<Anomaly> detect(
            DruidCluster cluster,
            JobMetadata jobMetadata
    ) throws SherlockException, DruidException {
        Granularity granularity = Granularity.getValue(jobMetadata.getGranularity());
        Query query = queryService.build(jobMetadata.getQuery(), granularity, jobMetadata.getGranularityRange(), jobMetadata.getEffectiveQueryTime(), jobMetadata.getTimeseriesRange());
        log.info("Query generation successful.");
        // reconstruct DetectorConfig
        DetectorConfig config = DetectorConfig.fromProperties(DetectorConfig.fromFile());
        config.setTsModel(jobMetadata.getTimeseriesModel());
        config.setAdModel(jobMetadata.getAnomalyDetectionModel());
        if (jobMetadata.getTimeseriesModel().equals(DetectorConfig.Framework.Prophet.toString())) {
            config.setTsFramework(DetectorConfig.Framework.Prophet.toString());
            config.setProphetGrowthModel(jobMetadata.getProphetGrowthModel());
            config.setProphetYearlySeasonality(jobMetadata.getProphetYearlySeasonality());
            config.setProphetWeeklySeasonality(jobMetadata.getProphetWeeklySeasonality());
            config.setProphetDailySeasonality(jobMetadata.getProphetDailySeasonality());
            log.info("DetectorConfig reconstructed with Prophet parameters.");
        } else {
            config.setTsFramework(DetectorConfig.Framework.Egads.toString());
            log.info("DetectorConfig reconstructed with Egads parameters.");
        }
        return detect(query, jobMetadata.getSigmaThreshold(), cluster, config, jobMetadata.getFrequency(), jobMetadata.getGranularityRange());
    }

    /**
     * Check to ensure that the datasource in the query exists
     * in the specified cluster.
     *
     * @param query   the query to check
     * @param cluster the druid cluster to check
     * @throws DruidException if the datasource is not found
     */
    public void checkDatasource(Query query, DruidCluster cluster) throws DruidException {
        ArrayList<String> inValidDataSources = new ArrayList<>();
        JsonElement datasourceInfo = query.getDatasource();
        JsonArray druidDataSources = httpService.queryDruidDatasources(cluster);
        if (datasourceInfo.isJsonArray()) {
            JsonArray dataSources = datasourceInfo.getAsJsonArray();
            for (JsonElement dataSource :
                    dataSources) {
                if (!druidDataSources.contains(dataSource)) {
                    inValidDataSources.add(dataSource.getAsString());
                }
            }
        } else {
            if (!druidDataSources.contains(datasourceInfo)) {
                inValidDataSources.add(datasourceInfo.getAsString());
            }
        }

        if (inValidDataSources.size() > 0) {
            log.error("Druid datasource {} does not exist!", inValidDataSources.toString());
            throw new DruidException("Querying unknown datasource: " + inValidDataSources.toString());
        }
    }

    /**
     * Send the query to druid and return the parsed JSON array
     * response to the caller.
     *
     * @param query   the query to execute
     * @param cluster the cluster to query
     * @return the parsed response
     * @throws DruidException if an error occurs while calling druid
     */
    public JsonArray queryDruid(Query query, DruidCluster cluster) throws DruidException {
        JsonArray druidResponse = httpService.queryDruid(cluster, query.getQueryJsonObject());
        log.info("Druid response received successfully");
        log.debug("Response from Druid is: {}", druidResponse);
        if (druidResponse.size() == 0) {
            log.error("Query to Druid returned empty response!");
        }
        return druidResponse;
    }

    /**
     * Run the detection job on a predefined query.
     *
     * @param druidResponse    the response from druid
     * @param query            the query that was used
     * @param sigmaThreshold   the job sigma threshold
     * @param granularityRange granularity range to aggregate on
     * @return the anomaly list from egads
     * @throws SherlockException if an error occurs during analysis
     */
    public List<Anomaly> runDetection(
            JsonArray druidResponse,
            Query query,
            Double sigmaThreshold,
            Integer granularityRange
    ) throws SherlockException {
        return runDetection(druidResponse, query, sigmaThreshold, null, null, granularityRange);
    }

    /**
     * Run detection with a provided Detector configuration and
     * Druid query.
     *
     * @param druidResponse    response from Druid
     * @param query            the Druid query
     * @param sigmaThreshold   job sigma threshold
     * @param config           Detector configuration
     * @param frequency        frequency of the job
     * @param granularityRange granularity range to aggregate on
     * @return anomalies from detection
     * @throws SherlockException if an error occurs during analysis
     */
    public List<Anomaly> runDetection(
            JsonArray druidResponse,
            Query query,
            Double sigmaThreshold,
            DetectorConfig config,
            String frequency,
            Integer granularityRange
    ) throws SherlockException {
        List<TimeSeries> timeSeriesList = parserService.parseTimeSeries(druidResponse, query);
        // The value of the last timestamp expected to be returned by Druid
        Integer expectedEnd = (query.getRunTime() / 60) - (query.getGranularity().getMinutes() * granularityRange);
        log.info("Expected timestamp of last data point in timeseries: {}", TimeUtils.getFormattedTimeMinutes(expectedEnd));
        List<Anomaly> anomalies = runDetection(timeSeriesList, sigmaThreshold, config, expectedEnd, frequency, query.getGranularity(), granularityRange);
        log.info("Generated anomaly list with {} anomalies", anomalies.size());
        return anomalies;
    }

    /**
     * Run detection on a list of time series (used by each DetectionTask).
     *
     * @param timeSeriesList   time series to analyze
     * @param sigmaThreshold   job sigma threshold
     * @param detectorConfig   the Detector configuration
     * @param endTimeMinutes   the expected last data point time in minutes
     * @param frequency        frequency of the job
     * @param granularity      granularity of druid query
     * @param granularityRange granularity range to aggregate on
     * @return list of anomalies from the detection job
     * @throws SherlockException if an error occurs during analysis
     */
    public synchronized List<Anomaly> runDetection(
            List<TimeSeries> timeSeriesList,
            Double sigmaThreshold,
            DetectorConfig detectorConfig,
            Integer endTimeMinutes,
            String frequency,
            Granularity granularity,
            Integer granularityRange
    ) throws SherlockException {
        DetectorAPIService detectorAPIService;
        List<Anomaly> anomalies = new ArrayList<>();
        // if detectorConfig is null, use egads for anomaly detection
        if (detectorConfig == null) {
            detectorAPIService = egadsAPIService;
        } else if (detectorConfig.getTsFramework().equals(DetectorConfig.Framework.Prophet.toString())) {
            detectorAPIService = prophetAPIService;
            detectorAPIService.configureWith(detectorConfig);
        } else if (detectorConfig.getTsFramework().equals(DetectorConfig.Framework.Egads.toString())) {
            if (DetectorConfig.TimeSeriesModel.getAllEgadsValues().contains(detectorConfig.getTsModel())) {
                detectorAPIService = egadsAPIService;
                detectorAPIService.configureWith(detectorConfig);
            } else {
                throw new IllegalArgumentException("Egads Time Series Forecasting Model not identified.");
            }
        } else {
            throw new IllegalArgumentException("Time Series Framework not identified.");
        }
        // Edge case: if time series list is empty, mark anomalies as NODATA
        if (timeSeriesList.isEmpty()) {
            anomalies.add(detectorAPIService.getNoDataAnomaly(new TimeSeries()));
            return anomalies;
        }
        // Set Standard deviation and Olympic Model's Base Window
        detectorAPIService.preRunConfigure(sigmaThreshold, granularity, granularityRange);
        // Set the detection window for anomaly detection models
        detectorAPIService.configureDetectionWindow(endTimeMinutes, frequency, granularityRange);
        // Anomaly detection
        anomalies.addAll(detectorAPIService.detectAnomalies(timeSeriesList, endTimeMinutes));
        log.info("Anomaly points added to result.");
        return anomalies;
    }

    /**
     * Run a detection job with a provided EGADS configuration.
     *
     * @param query            Druid query to use
     * @param sigmaThreshold   Job sigma threshold
     * @param cluster          Druid cluster to query
     * @param config           Detector's configuration
     * @param frequency        Frequency of the job
     * @param granularityRange granularity range to aggregate on
     * @return list of anomalies from the detection
     * @throws SherlockException if an error occurs during analysis
     * @throws DruidException    if an error occurs while contacting Druid
     */
    public List<Anomaly> detect(
            Query query,
            Double sigmaThreshold,
            DruidCluster cluster,
            DetectorConfig config,
            String frequency,
            Integer granularityRange
    ) throws SherlockException, DruidException {
        checkDatasource(query, cluster);
        JsonArray druidResponse = queryDruid(query, cluster);
        return runDetection(druidResponse, query, sigmaThreshold, config, frequency, granularityRange);
    }

    /**
     * Perform a Detector detection and return the results
     * as an {@code DetectorResult}.
     *
     * @param query           druid query
     * @param sigmaThreshold  sigma threshold to use
     * @param cluster         the druid cluster to query
     * @param detectionWindow detection window for anomalies
     * @param config          detectorConfig
     * @return a list of DetectorResult objects
     * @throws SherlockException if an error during processing occurs
     * @throws DruidException    if an error during querying occurs
     * @throws Exception if an error during toJSON occurs
     */
    public List<DetectorResult> detectWithResults(
            Query query,
            Double sigmaThreshold,
            DruidCluster cluster,
            @Nullable Integer detectionWindow,
            @Nonnull DetectorConfig config
    ) throws SherlockException, DruidException, Exception {
        DetectorAPIService detectorAPIService;
        checkDatasource(query, cluster);
        JsonArray druidResponse = queryDruid(query, cluster);
        List<TimeSeries> timeSeriesList = parserService.parseTimeSeries(druidResponse, query);
        List<DetectorResult> results = new ArrayList<>(timeSeriesList.size());
        if (config.getTsFramework().equals(DetectorConfig.Framework.Prophet.toString())) {
            detectorAPIService = prophetAPIService;
        } else if (config.getTsFramework().equals(DetectorConfig.Framework.Egads.toString())) {
            if (DetectorConfig.TimeSeriesModel.getAllEgadsValues().contains(config.getTsModel())) {
                detectorAPIService = egadsAPIService;
            } else {
                throw new IllegalArgumentException("Egads Time Series Forecasting Model not identified.");
            }
        } else {
            throw new IllegalArgumentException("Time Series Framework not identified.");
        }
        detectorAPIService.configureWith(config);
        detectorAPIService.preRunConfigure(sigmaThreshold, query.getGranularity(), query.getGranularityRange());
        if (detectionWindow != null) {
            detectorAPIService.configureDetectionWindow(query.getRunTime() / 60, query.getGranularity().toString(), detectionWindow + 1);
        }
        results.addAll(detectorAPIService.detectAnomaliesAndForecast(timeSeriesList));
        return results;
    }
}
