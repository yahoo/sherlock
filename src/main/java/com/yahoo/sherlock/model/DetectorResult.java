/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.model;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.settings.Constants;

import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;

/**
 * An {@code DetectorResult} contains the results of an
 * Anomaly Detection analysis, including a list of anomalies, the
 * original time series data, and the forecasted data sequence
 * using Egads/Prophet framework.
 */
@Data
public class DetectorResult {

    /**
     * A single data point, consisting of an {@code x}
     * value that is the point time and a {@code y}
     * value that is the point value.
     */
    @Data
    public static class Point {
        /**
         * Point timestamp in seconds.
         */
        private long x;
        /**
         * Point value.
         */
        private Float y;

        /**
         * Default constructor.
         */
        public Point() {
        }

        /**
         * Constructor for point.
         *
         * @param x timestamp value
         * @param y point value
         */
        public Point(long x, Float y) {
            this.x = x;
            this.y = y;
        }
    }

    /**
     * A {@code Series} represents a series of {@code Point}
     * objects and has an associated name.
     */
    @Data
    public static class Series {
        /**
         * The series data points.
         */
        private Point[] values;
        /**
         * The same of the series.
         */
        private String key;
        /**
         * The series index. Aliased as 'classed' since only
         * certain attributes are accepted by nvd3. Used to
         * highlight anomaly series.
         */
        private String classed;

        /**
         * Default constructor.
         */
        public Series() {
        }

        /**
         * Constructor for series.
         *
         * @param values the series values
         * @param key    the series name
         * @param index  the logical index of the series
         */
        public Series(Point[] values, String key, int index) {
            this.values = values;
            this.key = key;
            this.classed = "series-" + index;
        }

        /**
         * Set the index of the series.
         *
         * @param index the index
         * @return this series
         */
        public Series index(int index) {
            this.classed = "series-" + index;
            return this;
        }

    }

    /**
     * The list of anomalies for the detector result.
     */
    private List<Anomaly> anomalies;
    /**
     * The original time series data.
     */
    private TimeSeries timeseries;
    /**
     * The Time Series framework forecasted series.
     */
    private TimeSeries.DataSequence forecasted;

    /**
     * Default constructor.
     */
    public DetectorResult() {
    }

    /**
     * Constructor for Detector result.
     *
     * @param anomalies  the detected anomalies
     * @param timeseries the original time series
     * @param forecasted the model forecasted time series
     */
    public DetectorResult(List<Anomaly> anomalies, TimeSeries timeseries, TimeSeries.DataSequence forecasted) {
        this.anomalies = anomalies;
        this.timeseries = timeseries;
        this.forecasted = forecasted;
    }

    /**
     * Get the name of a time series.
     *
     * @return the name of the time series
     */
    public String getBaseName() {
        return timeseries.meta.name + Constants.SEMICOLON_DELIMITER + " " + timeseries.meta.source.replace(Constants.NEWLINE_DELIMITER, Constants.COMMA_DELIMITER);
    }

    /**
     * Get the data of the Detector result as {@code Series}.
     * The series are the original data, the predicted data,
     * and the anomalies.
     * <p>
     * This method will traverse all the series simulataneously
     * in order to ensure that each as a data point, which may be
     * of null value, for every time. This is because nvd3 poorly
     * handles missing data points, as opposed to null points.
     *
     * @return an array of Series
     */
    public Series[] getData() {
        String baseName = this.getBaseName();
        Series[] seriesArray = new Series[3];
        Anomaly.IntervalSequence anomalySeq =
                anomalies.isEmpty()
                        ? new Anomaly.IntervalSequence()
                        : anomalies.get(0).intervals;
        // Need to maintain iteration order
        LinkedHashMap<Long, Float> origValues = new LinkedHashMap<>((int) (timeseries.data.size() * 1.8));
        LinkedHashMap<Long, Float> predValues = new LinkedHashMap<>((int) (forecasted.size() * 1.8));
        LinkedHashMap<Long, Float> anomValues = new LinkedHashMap<>((int) (anomalySeq.size() * 1.8));
        for (TimeSeries.Entry entry : timeseries.data) {
            origValues.put(entry.time, entry.value);
        }
        for (TimeSeries.Entry entry : forecasted) {
            predValues.put(entry.time, entry.value);
        }
        for (Anomaly.Interval interval : anomalySeq) {
            anomValues.put(interval.startTime, interval.actualVal);
        }
        // The estimated time series has all the points
        int totalPoints = predValues.size();
        Point[] origPoints = new Point[totalPoints];
        Point[] predPoints = new Point[totalPoints];
        Point[] anomPoints = new Point[totalPoints];
        Iterator<Long> predKeyIt = predValues.keySet().iterator();
        for (int i = 0; i < totalPoints; i++) {
            // Map will return null if time does not exist
            Long time = predKeyIt.next();
            predPoints[i] = new Point(time, predValues.get(time));
            origPoints[i] = new Point(time, origValues.get(time));
            anomPoints[i] = new Point(time, anomValues.get(time));
        }
        seriesArray[0] = new Series(origPoints, "Original - " + baseName, 0);
        seriesArray[1] = new Series(predPoints, "Predicted - " + baseName, 0);
        seriesArray[2] = new Series(anomPoints, "Anomaly - " + baseName, 0);
        return seriesArray;
    }

    /**
     * Data are ordered as [orig, pred, anomaly, ...], so this method
     * will pair original and predicted data and put anomaly data at the
     * end of the array.
     *
     * @param series the series array to reorder
     * @return reordered series array
     */
    public static Series[] reorderData(Series[] series) {
        if (series.length % 3 != 0) {
            throw new IllegalArgumentException("Series length should be a multiple of 3");
        }
        Series[] reordered = new Series[series.length];
        int anomalyStart = (series.length * 2) / 3;
        int i = 0;
        for (int j = 0; j < series.length; j += 3, i += 2) {
            reordered[i] = series[j].index(i);
            reordered[i + 1] = series[j + 1].index(i + 1);
            reordered[anomalyStart + i / 2] = series[j + 2].index(anomalyStart + i / 2);
        }
        return reordered;
    }

    /**
     * Get the data of a list of Detector results in a single array.
     *
     * @param results list of results
     * @return all data in a series array
     */
    public static Series[] fuseResults(List<DetectorResult> results) {
        List<Series[]> data = new ArrayList<>(results.size());
        int totalSize = 0;
        for (DetectorResult result : results) {
            Series[] datum = result.getData();
            data.add(datum);
            totalSize += datum.length;
        }
        Series[] fusedData = new Series[totalSize];
        int i = 0;
        for (Series[] datum : data) {
            for (Series series : datum) {
                fusedData[i++] = series;
            }
        }
        return reorderData(fusedData);
    }

    /**
     * Convert the DetectorResult to a string.
     * @return an DetectorResult string
     */
    @Override
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("TimeSeries: \n");
        for (int i = 0; i < timeseries.size(); i++) {
            if (i > 0) {
                str.append(",");
            }
            str.append("[" + timeseries.time(i) + ": " + timeseries.value(i) + "]");
        }
        str.append("\n Forecasted: \n");
        for (int i = 0; i < forecasted.size(); i++) {
            if (i > 0) {
                str.append(",");
            }
            str.append("[" + forecasted.get(i).time + ": " + forecasted.get(i).value + "]");
        }
        str.append("\n Anomalies: \n");
        for (int i = 0; i < anomalies.size(); i++) {
            str.append(anomalies.get(i).toString());
        }
        return str.toString();
    }

    /**
     * Determines if the current DetectorResult instance equals to the other DetectorResult instance.
     * Ignores original and forecasted series' logical index since logical index is not used in Sherlock.
     * @return true if equal, false if not equal
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DetectorResult)) {
            return false;
        }
        DetectorResult newOther = (DetectorResult) other;
        // current time series' times and values
        Long[] ts1Times = this.timeseries.data.getTimes();
        Long[] ts2Times = newOther.getTimeseries().data.getTimes();
        Float[] ts1Vals = this.timeseries.data.getValues();
        Float[] ts2Vals = newOther.getTimeseries().data.getValues();
        // forecasted time series' times and values
        Long[] forecasted1Times = this.forecasted.getTimes();
        Long[] forecasted2Times = newOther.getForecasted().getTimes();
        Float[] forecasted1Vals = this.forecasted.getValues();
        Float[] forecasted2Vals = newOther.getForecasted().getValues();
        return this.anomalies.equals(newOther.anomalies)
                && Arrays.equals(ts1Times, ts2Times) && Arrays.equals(ts1Vals, ts2Vals)
                && Arrays.equals(forecasted1Times, forecasted2Times) && Arrays.equals(forecasted1Vals, forecasted2Vals);
    }
}
