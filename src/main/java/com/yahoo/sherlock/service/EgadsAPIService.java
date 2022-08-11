/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.egads.control.ProcessableObject;
import com.yahoo.egads.control.ProcessableObjectFactory;
import com.yahoo.egads.control.ModelAdapter;
import com.yahoo.egads.control.AnomalyDetector;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.DetectorResult;
import com.yahoo.sherlock.utils.EgadsUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Service class for Egads API.
 */
@Slf4j
@Data
public class EgadsAPIService extends DetectorAPIService {

    /**
     * Implementation of the detectAnomalies() abstract method.
     * It iterates a list of series, calls the core method, skip
     * time-series with unmatched end time, and generates a list
     * of anomalies.
     *
     * @param timeSeriesList a list of series
     * @param endTimeMinutes the end time for the detection task
     * @return list of anomalies
     * @throws SherlockException exception
     */
    @SuppressWarnings("unchecked")
    public List<Anomaly> detectAnomalies(List<TimeSeries> timeSeriesList, Integer endTimeMinutes) throws SherlockException {
        List<Anomaly> anomalies = new ArrayList<>();
        try {
            for (TimeSeries ts : timeSeriesList) {
                if (ts.data.isEmpty() || ts.time(ts.size() - 1) != endTimeMinutes * 60L) {
                    anomalies.add(getNoDataAnomaly(ts));
                } else {
                    anomalies.addAll(detectAnomalies(ts));
                }
            }
        } catch (Exception e) {
            log.error("Error in EGADS!", e);
            throw new SherlockException(e.getMessage());
        }
        return anomalies;
    }

    /**
     * Core method that runs Egads API to detect anomalies on a single series.
     *
     * @param timeseries a single timeseries
     * @return list of anomalies
     * @throws SherlockException exception
     */
    @SuppressWarnings("unchecked")
    protected List<Anomaly> detectAnomalies(TimeSeries timeseries) throws SherlockException {
        List<Anomaly> anomalies = new ArrayList<>();
        try {
            ProcessableObject processableObject = getEgadsProcessableObject(timeseries);
            processableObject.process();
            anomalies.addAll((ArrayList<Anomaly>) processableObject.result());
        } catch (Exception e) {
            log.error("Exception from EGADS!", e);
            throw new SherlockException(e.getMessage());
        }
        return anomalies;
    }

    /**
     * Implementation of the detectAnomaliesAndForecast() abstract method.
     * It iterates a list of series, calls the core method, and
     * generates a list of anomalies.
     *
     * @param timeSeriesList a list of time series to analyze
     * @return a list of DetectorResult objects
     * @throws SherlockException if an error occurs during detection
     */
    public List<DetectorResult> detectAnomaliesAndForecast(List<TimeSeries> timeSeriesList) throws SherlockException {
        List<DetectorResult> results = new ArrayList<>(timeSeriesList.size());
        for (TimeSeries timeSeries : timeSeriesList) {
            results.add(detectAnomaliesAndForecast(timeSeries));
        }
        return results;
    }

    /**
     * Core method that runs Egads API to generate DetectorResult based on a single series.
     *
     * @param timeseries the time series to analyze
     * @return an DetectorResult object containing anomalies, the
     * original time series, and forecasted data sequence
     * @throws SherlockException if an error occurs during detection
     */
    public DetectorResult detectAnomaliesAndForecast(TimeSeries timeseries) throws SherlockException {
        try {
            // For now, instant query will show all anomalies on the graph
            p.setProperty(MAX_ANOMALY_TIME_AGO, DEFAULT_MAX_ANOMALY_TIME);
            // train the model
            ModelAdapter modelAdapter = EgadsUtils.getTSModel(timeseries, p);
            modelAdapter.reset();
            modelAdapter.train();
            List<TimeSeries.DataSequence> expected = modelAdapter.forecast(timeseries.time(0), timeseries.time(timeseries.size() - 1));
            // detect anomalies
            AnomalyDetector anomalyDetector = EgadsUtils.getAnomalyDetector(timeseries, p);
            List<Anomaly> anomalies  = getAnomalies(anomalyDetector, timeseries, expected.get(0));
            return new DetectorResult(anomalies, timeseries, expected.get(0));
        } catch (Exception e) {
            log.error("Error in EGADS!", e);
            throw new SherlockException(e.getMessage(), e);
        }
    }

    /**
     * Method to get processable object from Egads.
     *
     * @param timeseries input timeseries
     * @return ProcessableObject instance
     * @throws SherlockException exception in fillMissingData
     */
    protected ProcessableObject getEgadsProcessableObject(TimeSeries timeseries) throws SherlockException {
        return ProcessableObjectFactory.create(EgadsUtils.fillMissingData(timeseries, p), p);
    }

}
