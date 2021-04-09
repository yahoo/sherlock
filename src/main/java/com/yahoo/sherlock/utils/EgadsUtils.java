/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.egads.control.DetectAnomalyProcessable;
import com.yahoo.egads.control.ModelAdapter;
import com.yahoo.egads.control.ProcessableObject;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.exception.SherlockException;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Egads Utility functions.
 */
@Slf4j
public class EgadsUtils {

    /**
     * Name postfix '_aggr_ ' for aggregated timeseries.
     */
    private static final String AGGR = "_aggr_";


    /**
     * Fill in the missing data in a time series.
     *
     * @param timeseries  the time series to fill
     * @param aggr        the level of aggregation
     * @param fillMissing whether the method should fill in missing data
     * @return processed time series
     * @throws SherlockException exception
     */
    public static TimeSeries fillMissingData(TimeSeries timeseries, int aggr, int fillMissing) throws SherlockException {
        TimeSeries output = new TimeSeries();
        Long interval = timeseries.mostFrequentPeriod();
        output.meta = timeseries.meta;

        // sanity check
        if (interval == 0L) {
            // throw exception
            throw new SherlockException("Most frequent periods(granularity):" + interval);
        }
        log.debug("starting filling...");
        if (fillMissing == 1) {
            for (int i = 1; i < timeseries.size(); i++) {
                if (timeseries.data.get(i).time - timeseries.data.get(i - 1).time != interval) {
                    int missingPoints = (int) ((timeseries.data.get(i).time - timeseries.data.get(i - 1).time) / interval);
                    Long curTimestampToFill = timeseries.data.get(i - 1).time;
                    for (int j = missingPoints; j > 0; j--) {
                        try {
                            output.append(curTimestampToFill, timeseries.value(i - 1));
                        } catch (Exception e) {
                            log.error("Error while filling missing data in timeseries!", e);
                        }
                        curTimestampToFill += interval;
                    }
                } else {
                    try {
                        output.append(timeseries.time(i - 1), timeseries.value(i - 1));
                    } catch (Exception e) {
                        log.error("Error while filling missing data in timeseries!", e);
                    }
                }
            }
            try {
                output.append(timeseries.time(timeseries.size() - 1), timeseries.value(timeseries.size() - 1));
            } catch (Exception e) {
                log.error("Error while filling missing data in timeseries!", e);
            }
        } else {
            return timeseries;
        }

        // Handle aggregation.
        if (aggr > 1) {
            output.data = sumAggregator(output, aggr);
        }
        return output;
    }

    /**
     * Sum aggregator for datapoints.
     * @param timeSeries input timeseries
     * @param frequency aggregation frequency
     * @return aggregated timeseries
     */
    public static TimeSeries.DataSequence sumAggregator(TimeSeries timeSeries, int frequency) {
        TimeSeries.DataSequence output = new TimeSeries.DataSequence();

        for (int i = 0; i < timeSeries.data.size(); i += frequency) {
            Float aggr = 0.0F;
            Long time = (timeSeries.data.get(i)).time;
            for (int j = i; j < Math.min(timeSeries.data.size(), i + frequency); ++j) {
                aggr = aggr + (timeSeries.data.get(j)).value;
            }
            output.add(new TimeSeries.Entry(time, aggr));
        }
        return output;
    }

    /**
     * Method to fill in missing datapoints in timeseries if any.
     *
     * @param timeseries input timeseries
     * @param p          properties of egads
     * @return complete timeseries
     * @throws SherlockException exception
     */
    public static TimeSeries fillMissingData(TimeSeries timeseries, Properties p) throws SherlockException {
        int aggr;
        if (!NumberUtils.isNonNegativeInt(p.getProperty("AGGREGATION"))) {
            aggr = 1;
        } else {
            aggr = Integer.parseInt(p.getProperty("AGGREGATION"));
        }
        int fillMissing;
        if (p.getProperty("FILL_MISSING").equals("1")) {
            fillMissing = 1;
        } else {
            fillMissing = 0;
        }
        return fillMissingData(timeseries, aggr, fillMissing);
    }

    /**
     * Use reflection to acquire the model adapter
     * field in a {@code DetectAnomalyProcessable}.
     *
     * @param processableObject the processable object
     * @return its model adapter, which may be null
     */
    public static ModelAdapter getModelAdapter(ProcessableObject processableObject) {
        if (!(processableObject instanceof DetectAnomalyProcessable)) {
            return null;
        }
        DetectAnomalyProcessable anomalyProcessable = (DetectAnomalyProcessable) processableObject;
        try {
            Field modelAdapterField = DetectAnomalyProcessable.class.getDeclaredField("ma");
            modelAdapterField.setAccessible(true);
            return (ModelAdapter) modelAdapterField.get(anomalyProcessable);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return null;
        }
    }

    /**
     * Use a model adapter to forecase a provided time series.
     * The model adapter must have already been trained on the
     * provided time series.
     *
     * @param timeseries   the time series to forecase
     * @param modelAdapter the model adapter to use
     * @return a list of data sequences that represented each model's
     * projection in the model adapter.
     */
    public static List<TimeSeries.DataSequence> getModelForecast(TimeSeries timeseries, ModelAdapter modelAdapter) {
        try {
            return modelAdapter.forecast(timeseries.startTime(), timeseries.lastTime());
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}
