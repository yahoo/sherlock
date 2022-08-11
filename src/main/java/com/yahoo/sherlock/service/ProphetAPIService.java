/*
 * Copyright 2022, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.control.AnomalyDetector;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.DetectorResult;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.google.gson.Gson;
import com.yahoo.sherlock.utils.EgadsUtils;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.yahoo.egads.data.TimeSeries.Entry;
import com.yahoo.egads.data.Anomaly;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Map;

/**
 * Service class for Prophet API.
 */
@NoArgsConstructor
@Slf4j
public class ProphetAPIService extends DetectorAPIService  {

    /**
     * Constant for "PROPHET_GROWTH_MODEL".
     */
    private static final String PROPHET_GROWTH_MODEL = "PROPHET_GROWTH_MODEL";

    /**
     * Constant for "PROPHET_YEARLY_SEASONALITY".
     */
    private static final String PROPHET_YEARLY_SEASONALITY = "PROPHET_YEARLY_SEASONALITY";

    /**
     * Constant for "PROPHET_WEEKLY_SEASONALITY".
     */
    private static final String PROPHET_WEEKLY_SEASONALITY = "PROPHET_WEEKLY_SEASONALITY";

    /**
     * Constant for "PROPHET_DAILY_SEASONALITY".
     */
    private static final String PROPHET_DAILY_SEASONALITY = "PROPHET_DAILY_SEASONALITY";

    /**
     * Constant for "URL_FORMAT".
     */
    private static final String URL_FORMAT = "%s://%s/%s";

    /**
     * Class HTTP service instance.
     */
    private HttpService httpService = new HttpService();

    /**
     * Constructor with httpService and properties.
     *
     * @param httpService httpService used in ProphetAPIService
     * @param p DetectorAPIService properties
     */
    public ProphetAPIService(HttpService httpService, Properties p) {
        this.httpService = httpService;
        this.p = p;
    }

    /**
     * Implementation of the detectAnomalies() abstract method.
     * It queries the Prophet microservice, skip time-series
     * with unmatched end time, and generates a list of anomalies.
     *
     * @param timeSeriesList a list of series
     * @param endTimeMinutes the end time for the detection task
     * @return list of anomalies
     * @throws SherlockException exception
     */
    @SuppressWarnings("unchecked")
    public List<Anomaly> detectAnomalies(List<TimeSeries> timeSeriesList, Integer endTimeMinutes) throws SherlockException {
        String prophetFullUrl = generateProphetURL();
        List<Anomaly> anomalies = new ArrayList<>();
        List<TimeSeries> filledTSList = new ArrayList<>();
        try {
            // fill missing data
            for (TimeSeries ts : timeSeriesList) {
                filledTSList.add(EgadsUtils.fillMissingData(ts, p));
            }
            // Convert time series to Json
            JsonObject jsonTSList = listParamsToJson(p.getProperty(PROPHET_GROWTH_MODEL), p.getProperty(PROPHET_YEARLY_SEASONALITY),
                    p.getProperty(PROPHET_WEEKLY_SEASONALITY), p.getProperty(PROPHET_DAILY_SEASONALITY), filledTSList);
            // query Prophet Service for expected time-series
            JsonObject expectedJsonList = httpService.queryProphetService(prophetFullUrl, jsonTSList);
            List<TimeSeries.DataSequence> expectedList = jsonToDataSequenceList(expectedJsonList);
            // Anomaly Detection
            for (int i = 0; i < filledTSList.size(); i++) {
                TimeSeries originalTS = timeSeriesList.get(i);
                if (originalTS.data.isEmpty() || originalTS.time(originalTS.size() - 1) != endTimeMinutes * 60L) {
                    anomalies.add(getNoDataAnomaly(originalTS));
                } else {
                    AnomalyDetector ad = EgadsUtils.getAnomalyDetector(filledTSList.get(i), p);
                    anomalies.addAll(getAnomalies(ad, filledTSList.get(i), expectedList.get(i)));
                }
            }
        } catch (Exception e) {
            log.error("Error in forecasting expected time series via Prophet Service", e);
            throw new SherlockException(e.getMessage(), e);
        }
        log.info("Prophet anomaly detection completed.");
        return anomalies;
    }

    /**
     * Implementation of the detectAnomaliesAndForecast() abstract method.
     * It iterates a list of series, queries the Prophet microservice,
     * and generates a list of anomalies.
     *
     * @param timeSeriesList a list of time series to analyze
     * @return a list of DetectorResult objects
     * @throws SherlockException if an error occurs during detection
     */
    public List<DetectorResult> detectAnomaliesAndForecast(List<TimeSeries> timeSeriesList) throws SherlockException {
        p.setProperty(MAX_ANOMALY_TIME_AGO, DEFAULT_MAX_ANOMALY_TIME);
        String prophetFullUrl = generateProphetURL();
        List<DetectorResult> results = new ArrayList<>();
        try {
            // fill missing data in Time-series if applicable
            List<TimeSeries> filledTSList = new ArrayList<>();
            for (TimeSeries ts : timeSeriesList) {
                filledTSList.add(EgadsUtils.fillMissingData(ts, p));
            }
            // convert time series to Json
            JsonObject jsonTSList = listParamsToJson(p.getProperty(PROPHET_GROWTH_MODEL), p.getProperty(PROPHET_YEARLY_SEASONALITY),
                    p.getProperty(PROPHET_WEEKLY_SEASONALITY), p.getProperty(PROPHET_DAILY_SEASONALITY), filledTSList);
            // query Prophet Service for expected time-series
            JsonObject expectedJsonList = httpService.queryProphetService(prophetFullUrl, jsonTSList);
            List<TimeSeries.DataSequence> expectedList = jsonToDataSequenceList(expectedJsonList);
            // use Egads' Anomaly Detection Module to detect anomalies
            for (int i = 0; i < filledTSList.size(); i++) {
                AnomalyDetector ad = EgadsUtils.getAnomalyDetector(filledTSList.get(i), p);
                List<Anomaly> anomalies = getAnomalies(ad, filledTSList.get(i), expectedList.get(i));
                results.add(new DetectorResult(anomalies, filledTSList.get(i), expectedList.get(i)));
            }
        } catch (Exception e) {
            log.error("Error in forecasting expected time series via Prophet Service", e);
            throw new SherlockException(e.getMessage(), e);
        }
        log.info("Prophet anomaly detection completed.");
        return results;
    }

    /**
     *
     * Convert a list of time-series entries to a JsonObject according to Prophet Microservice required format.
     *
     * @param growthModel       growth trend used in Meta's Prophet service
     * @param yearlySeasonality the yearly seasonality assumption ("auto"/"true"/"false")
     * @param weeklySeasonality the weekly seasonality assumption ("auto"/"true"/"false")
     * @param dailySeasonality  the daily seasonality assumption ("auto"/"true"/"false")
     * @param timeSeriesList    list of time series data
     * @return a JSONObject used to query Prophet Service
     */
    public JsonObject listParamsToJson(String growthModel, String yearlySeasonality, String weeklySeasonality, String dailySeasonality, List<TimeSeries> timeSeriesList) {
        JsonObject output = new JsonObject();
        output.addProperty("growth", growthModel);
        output.addProperty("yearly_seasonality", yearlySeasonality);
        output.addProperty("weekly_seasonality", weeklySeasonality);
        output.addProperty("daily_seasonality", dailySeasonality);
        JsonArray tsArray = new JsonArray();
        for (TimeSeries ts : timeSeriesList) {
            JsonObject tsObject = new JsonObject();
            for (Entry entry : ts.data) {
                tsObject.addProperty(String.valueOf(entry.time), entry.value);
            }
            tsArray.add(tsObject);
        }
        output.add("timeseries", new Gson().toJsonTree(tsArray));
        return output;
    }

    /**
     * Converts json object response from Prophet Service to
     * a list of forecasted Data Sequence.
     *
     * @param jsonObject  JsonObject received from the Prophet Service
     * @return forecasted TimeSeries.DataSequence
     */
    public List<TimeSeries.DataSequence> jsonToDataSequenceList(JsonObject jsonObject) {
        List<TimeSeries.DataSequence> result = new ArrayList<>();
        JsonArray tsList = jsonObject.getAsJsonArray("forecasted");
        for (int i = 0; i < tsList.size(); i++) {
            JsonObject ts = tsList.get(i).getAsJsonObject();
            TimeSeries.DataSequence tmp = new TimeSeries.DataSequence();
            for (Map.Entry<String, JsonElement> tsEntry : ts.entrySet()) {
                tmp.add(new Entry(Long.parseLong(tsEntry.getKey()), tsEntry.getValue().getAsFloat()));
            }
            result.add(tmp);
        }
        return result;
    }

    /**
     * Generates a full Prophet Service URL from existing constants.
     *
     * @return full Prophet Service URL
     */
    public String generateProphetURL() {
        return String.format(URL_FORMAT, Constants.HTTP, CLISettings.PROPHET_URL, Constants.PROPHET_ENDPOINT);
    }

}
