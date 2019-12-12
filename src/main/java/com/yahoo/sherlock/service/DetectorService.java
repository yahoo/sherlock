/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.utils.TimeUtils;

import lombok.extern.slf4j.Slf4j;

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
     * Class EGADs service instance.
     */
    private EgadsService egads = new EgadsService();

    /**
     * Constant for AD_MODEL egads property.
     */
    private static final String AD_MODEL = "AD_MODEL";

    /**
     * Empty constructor.
     */
    public DetectorService() {
    }

    /**
     * Method to detect anomalies.
     * This method handles the control/data flow between the components of detection system.
     *
     * @param cluster           the Druid query to issue the query
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
        return detect(query, jobMetadata.getSigmaThreshold(), cluster, jobMetadata.getFrequency(), jobMetadata.getGranularityRange());
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
        JsonElement datasourceInfo = query.getDatasource();
        ArrayList<String> inValidDataSources = new ArrayList<>();
        JsonArray druidDataSources = httpService.queryDruidDatasources(cluster);
        boolean isValidDataSource = true;
        if (datasourceInfo.isJsonArray()) {
            JsonArray dataSources = datasourceInfo.getAsJsonArray();
            for (JsonElement dataSource :
                    dataSources) {
                if (!druidDataSources.contains(dataSource)) {
                    isValidDataSource = false;
                    inValidDataSources.add(dataSource.getAsString());
                }
            }
        } else {
            if (!druidDataSources.contains(datasourceInfo)) {
                isValidDataSource = false;
                inValidDataSources.add(datasourceInfo.getAsString());
            }
        }

        if (!isValidDataSource) {
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
     * Run detection with a provided EGADS configuration and
     * Druid query.
     *
     * @param druidResponse    response from Druid
     * @param query            the Druid query
     * @param sigmaThreshold   job sigma threshold
     * @param config           EGADS configuration
     * @param frequency        frequency of the job
     * @param granularityRange granularity range to aggregate on
     * @return anomalies from detection
     * @throws SherlockException if an error occurs during analysis
     */
    public List<Anomaly> runDetection(
            JsonArray druidResponse,
            Query query,
            Double sigmaThreshold,
            EgadsConfig config,
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
     * Run detection on a list of time series.
     *
     * @param timeSeriesList   time series to analyze
     * @param sigmaThreshold   job sigma threshold
     * @param egadsConfig      the EGADS configuration
     * @param endTimeMinutes   the expected last data point time in minutes
     * @param frequency        frequency of the job
     * @param granularityRange granularity range to aggregate on
     * @return list of anomalies from the detection job
     * @throws SherlockException if an error occurs during analysis
     */
    public synchronized List<Anomaly> runDetection(
            List<TimeSeries> timeSeriesList,
            Double sigmaThreshold,
            EgadsConfig egadsConfig,
            Integer endTimeMinutes,
            String frequency,
            Granularity granularity,
            Integer granularityRange
    ) throws SherlockException {
        if (egadsConfig != null) {
            egads.configureWith(egadsConfig);
        } else {
            egads.preRunConfigure(sigmaThreshold, granularity, granularityRange);
        }
        // Configure the detection window for anomaly detection
        egads.configureDetectionWindow(endTimeMinutes, frequency, granularityRange);
        List<Anomaly> anomalies = new ArrayList<>(timeSeriesList.size());
        if (timeSeriesList.isEmpty()) {
            anomalies.add(getNoDataAnomaly(new TimeSeries()));
        }
        for (TimeSeries timeSeries : timeSeriesList) {
            if (timeSeries.data.isEmpty() ||
                    timeSeries.data.get(timeSeries.data.size() - 1).time != endTimeMinutes * 60L) {
                anomalies.add(getNoDataAnomaly(timeSeries));
            } else {
                anomalies.addAll(egads.runEGADS(timeSeries, sigmaThreshold));
            }
        }
        return anomalies;
    }

    /**
     * @param timeSeries time series for which to generate empty anomaly
     * @return an anomaly that represents no data
     */
    private Anomaly getNoDataAnomaly(TimeSeries timeSeries) {
        Anomaly anomaly = new Anomaly();
        anomaly.metricMetaData.name = JobStatus.NODATA.getValue();
        anomaly.metricMetaData.source = timeSeries.meta.source;
        anomaly.metricMetaData.id = timeSeries.meta.id;
        anomaly.id = timeSeries.meta.id;
        anomaly.intervals = new Anomaly.IntervalSequence();
        anomaly.modelName = (egads.getP() != null) ? egads.getP().getProperty(AD_MODEL) : "";
        return anomaly;
    }

    /**
     * Perform a detection job with a specified query
     * on a cluster.
     *
     * @param query            the query to use
     * @param sigmaThreshold   the sigma threshold for the detection
     * @param cluster          the cluster to query
     * @param frequency        frequency of the job
     * @param granularityRange granularity range to aggregate on
     * @return a list of anomalies
     * @throws SherlockException if an error occurs during detection
     * @throws DruidException    if an error occurs while contacting Druid
     */
    public List<Anomaly> detect(
            Query query,
            Double sigmaThreshold,
            DruidCluster cluster,
            String frequency,
            Integer granularityRange
    ) throws SherlockException, DruidException {
        return detect(query, sigmaThreshold, cluster, null, frequency, granularityRange);
    }

    /**
     * Run a detection job with a provided EGADS configuration.
     *
     * @param query            Druid query to use
     * @param sigmaThreshold   Job sigma threshold
     * @param cluster          Druid cluster to query
     * @param config           EGADS configuration
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
            EgadsConfig config,
            String frequency,
            Integer granularityRange
    ) throws SherlockException, DruidException {
        checkDatasource(query, cluster);
        JsonArray druidResponse = queryDruid(query, cluster);
        return runDetection(druidResponse, query, sigmaThreshold, config, frequency, granularityRange);
    }

    /**
     * Perform an egads detection and return the results
     * as an {@code EgadsResult}.
     *
     * @param query           druid query
     * @param sigmaThreshold  sigma threshold to use
     * @param cluster         the druid cluster to query
     * @param detectionWindow detection window for anomalies
     * @param config          the egads configuration
     * @return a list of egads results
     * @throws SherlockException if an error during processing occurs
     * @throws DruidException    if an error during querying occurs
     */
    public List<EgadsResult> detectWithResults(
            Query query,
            Double sigmaThreshold,
            DruidCluster cluster,
            @Nullable Integer detectionWindow,
            @Nullable EgadsConfig config
    ) throws SherlockException, DruidException {
        checkDatasource(query, cluster);
        JsonArray druidResponse = queryDruid(query, cluster);
        List<TimeSeries> timeSeriesList = parserService.parseTimeSeries(druidResponse, query);
        List<EgadsResult> results = new ArrayList<>(timeSeriesList.size());
        if (config != null) {
            egads.configureWith(config);
        }
        egads.preRunConfigure(sigmaThreshold, query.getGranularity(), query.getGranularityRange());
        if (detectionWindow != null) {
            egads.configureDetectionWindow(query.getRunTime() / 60, query.getGranularity().toString(), detectionWindow + 1);
        }
        for (TimeSeries timeSeries : timeSeriesList) {
            results.add(egads.detectAnomaliesResult(timeSeries));
        }
        return results;
    }
}
