/*
 * Copyright 2022, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.egads.control.AnomalyDetector;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.DetectorResult;
import com.yahoo.sherlock.query.DetectorConfig;
import com.yahoo.sherlock.settings.Constants;
import lombok.extern.slf4j.Slf4j;
import lombok.Data;

import java.util.List;
import java.util.Properties;
import com.yahoo.egads.data.Anomaly;

/**
 * Abstract Service class for all Detector API Services (Egads/Prophet).
 */
@Slf4j
@Data
public abstract class DetectorAPIService {

    private static final String BASE_WINDOWS = "BASE_WINDOWS";

    private static final String DETECTION_WINDOW_START_TIME = "DETECTION_WINDOW_START_TIME";

    protected static final String MAX_ANOMALY_TIME_AGO = "MAX_ANOMALY_TIME_AGO";

    protected static final String DEFAULT_MAX_ANOMALY_TIME = "99999999";

    public static final String THRESHOLD = "THRESHOLD";

    /**
     * Constant for "THRESHOLD_VAL"; used for anomaly detection via NaiveModel.
     */
    public static final String THRESHOLD_VAL = "mapee#100,mae#1000,smape#100,mape#10,mase#15";

    /**
     * Properties that stores the detector config.
     */
    protected Properties p = null;

    /**
     * An abstract method implemented separated in each DetectorAPIService class.
     * It runs an anomaly detection job, returning a list of DetectorResult that
     * contains a list of anomalies, the original time series data, and the
     * forecasted data sequence.
     *
     * @param timeSeriesList a list of time series to analyze
     * @return an Detector result object containing the above
     * @throws SherlockException if an error occurs during detection
     */
    public abstract List<DetectorResult> detectAnomaliesAndForecast(List<TimeSeries> timeSeriesList) throws SherlockException;

    /**
     * An abstract method implemented separated in each DetectorAPIService class.
     * It runs an anomaly detection job, returning a list of Amomalies.
     *
     * @param timeseries the time series to analyze
     * @param endTimeMinutes the end time of the detection task
     * @return an EGADS result object containing the above
     * @throws SherlockException if an error occurs during detection
     */
    public abstract List<Anomaly> detectAnomalies(List<TimeSeries> timeseries, Integer endTimeMinutes) throws SherlockException;

    /**
     * Default configuration of DetectorConfig.
     */
    public void init() {
        p = DetectorConfig.create().buildDefault().asProperties();
    }

    /**
     * Before-run Detector configuration. Ensures that
     * there is an existing properties object
     * and updates the base windows based
     * on the time series.
     * @param sigmaThreshold the sigma threshold
     * @param granularity timeseries granularity
     * @param granularityRange granularity range to aggregate on
     */
    public void preRunConfigure(Double sigmaThreshold, Granularity granularity, Integer granularityRange) {
        // If p is null, configure from config file; if that fails, load config from CLISettings default value
        if (p == null) {
            log.error("Egads properties have not been set! Attempting to load from file.");
            p = DetectorConfig.fromFile();
            if (p == null) {
                init();
            }
        }
        p.setProperty("AUTO_SENSITIVITY_SD", sigmaThreshold.toString());
        updateBaseWindow(granularity, granularityRange);
        configureThreshold();
    }

    /**
     * Configure the Detector API Service instance
     * with the default configuration.
     */
    public void configureWithDefault() {
        init();
    }

    /**
     * Method to update base window of Olympic model based on period of timeseries.
     *
     * @param granularity granularity of data
     * @param granularityRange granularity to aggregate on
     */
    private void updateBaseWindow(Granularity granularity, Integer granularityRange) {
        String window1 = "1", window2 = "1";
        switch (granularity) {
            case MINUTE:
                window2 = String.valueOf(Math.round(60.0f / granularityRange));
                break;
            case HOUR:
                window1 = String.valueOf(Math.round(24.0f / granularityRange));
                window2 = String.valueOf(Math.round(168.0f / granularityRange));
                break;
            case DAY:
                window2 = String.valueOf(Math.round(7.0f / granularityRange));
                break;
            case WEEK:
                window2 = String.valueOf(Math.round(4.0f / granularityRange));
                break;
            case MONTH:
                window2 = String.valueOf(Math.round(12.0f / granularityRange));
                p.setProperty("PERIOD", "-1");
                break;
            default:
                break;
        }
        p.setProperty(BASE_WINDOWS, window1 + Constants.COMMA_DELIMITER + window2);
        log.info("Updated BASE_WINDOWS: {}", p.getProperty(BASE_WINDOWS));
    }

    /**
     * Configure the current DetectorAPIService instance
     * with a provided configuration object.
     *
     * @param config DetectorConfig object
     */
    public void configureWith(DetectorConfig config) {
        p = config.asProperties();
        configureThreshold();
    }

    /**
     * Method to set the detection window for anomalies in the given job.
     * @param endTimeMinutes last datapoint timestamp in timeseries
     * @param frequency frequency of the job
     * @param nLookBack number of frequency to lookback
     */
    public void configureDetectionWindow(Integer endTimeMinutes, String frequency, int nLookBack) {
        Long detectionStartTime = (endTimeMinutes - nLookBack * Granularity.getValue(frequency).getMinutes()) * 60L;
        p.setProperty(DETECTION_WINDOW_START_TIME, detectionStartTime.toString());
    }

    /**
     * Method to compute the Anomalies via a given Egads Anomaly Detector instance.
     * @param anomalyDetector an Egads Anomaly Detector instance.
     * @param timeseries original timeseries
     * @param expected expected (forecasted) timeseries
     * @return anomalies detected by Egads Anomaly Detection Module
     * @throws Exception exception
     */
    public List<Anomaly> getAnomalies(AnomalyDetector anomalyDetector, TimeSeries timeseries, TimeSeries.DataSequence expected) throws Exception {
        anomalyDetector.reset();
        anomalyDetector.tune(expected);
        return anomalyDetector.detect(timeseries, expected);
    }

    /**
     * Generate an empty Anomly instance marked with NODATA based on a given time series.
     *
     * @param timeSeries time series for which to generate empty anomaly
     * @return an anomaly that represents no data
     */
    public Anomaly getNoDataAnomaly(TimeSeries timeSeries) {
        Anomaly anomaly = new Anomaly();
        anomaly.metricMetaData.name = JobStatus.NODATA.getValue();
        anomaly.metricMetaData.source = timeSeries.meta.source;
        anomaly.metricMetaData.id = timeSeries.meta.id;
        anomaly.id = timeSeries.meta.id;
        anomaly.intervals = new Anomaly.IntervalSequence();
        anomaly.modelName = (p != null) ? p.getProperty("AD_MODEL") : "";
        return anomaly;
    }

    /**
     * Helper method to set THRESHOLD if the anomaly detection model is NaiveModel; otherwise, NaiveModel will fail.
     */
    public void configureThreshold() {
        if (p != null && p.getProperty(DetectorConfig.AD_MODEL).equals(DetectorConfig.AnomalyDetectionModel.NaiveModel.toString())) {
            p.setProperty(THRESHOLD, THRESHOLD_VAL);
        }
    }
}
