/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.JsonArray;
import com.yahoo.sherlock.query.JsonTimeSeries;
import com.yahoo.sherlock.utils.EgadsUtils;
import com.yahoo.egads.data.MetricMeta;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.query.Query;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Service class to parse the druid response as a list of timeseries.
 */
@Slf4j
public class TimeSeriesParserService {

    /**
     * Method to generate timeseries from json array.
     *
     * @param timeseriesJson druid response timeseries JSON
     * @param query          associated query object
     * @return list of timeseries
     * @throws SherlockException exception in druid response parsing
     */
    public List<TimeSeries> parseTimeSeries(JsonArray timeseriesJson, Query query) throws SherlockException {
        log.info("Parsing druid response.");
        // list to store parsed timeseries
        List<TimeSeries> timeSeriesList = new ArrayList<>();
        // check for null jsonarray
        if (timeseriesJson == null) {
            log.error("Error in druid response.");
            throw new SherlockException("Null druid response!");
        } else {
            // deserialize timeseriesJson to JsonDataSequence
            JsonTimeSeries jsonTimeSeries = new JsonTimeSeries(timeseriesJson, query);
            log.info("Deserialization to json data sequence successful.");
            // process each datapoint for each groupby dimensions
            jsonTimeSeries.getJsonDataSequence().stream()
                .filter(JsonTimeSeries.combinedFilters())                                    // filter invalid datapoints
                .forEach(jsonDataPoint -> {
                        try {
                            jsonTimeSeries.processJsonDataPoint(jsonDataPoint);
                        } catch (Exception e) {
                            log.error("Error while processing data point!", e);
                        }
                    });
            jsonTimeSeries.getUniqueTimeSeriesMap()
                .values()
                .stream()
                .filter(isValidTimeSeries())
                .forEach(timeSeriesList::add);                      // get the list of timeseries
        }
        return timeSeriesList;
    }

    /**
     * "A temporary" filter for bad timeseries. Will make it robust in future. //TODO
     *
     * @return true if valid timeseries else false
     */
    public Predicate<TimeSeries> isValidTimeSeries() {
        return x -> x.size() >= 7;
    }

    /**
     * This method takes a list of time series for an entire time frame and
     * returns {@code fillIntervals} number of sub series lists that contain
     * a subset of the points in each of the source time series. This method
     * is used to parition Druid query results into time series for each
     * backfill period.
     *
     * @param sources     the source time series
     * @param start       start of backfill job window
     * @param end         end of backfill job window
     * @param granularity the data granularity
     * @return an array of time series lists
     */
    public List<TimeSeries>[] subseries(List<TimeSeries> sources, long start, long end, Granularity granularity) {
        long singleInterval = granularity.getMinutes() * granularity.getIntervalsFromSettings();
        int fillIntervals = (int) ((end - (start + singleInterval)) / granularity.getMinutes()) + 1;
        @SuppressWarnings("unchecked") List<TimeSeries>[] result = (List<TimeSeries>[]) new List[fillIntervals];
        if (sources.isEmpty()) {
            return result;
        }
        // sort the datapoints
        for (TimeSeries source : sources) {
            source.data.sort(Comparator.comparingLong(a -> a.time));
        }
        int intervalIndex = 0;
        // collect all subtimeseries
        for (long i = start ; i <= end && intervalIndex < fillIntervals ; i += granularity.getMinutes()) {
            final long localStart = i * 60;
            final long localEnd = (i + singleInterval) * 60;
            List<TimeSeries> subTimeseriesList;
            subTimeseriesList = sources.stream().map(source -> {
                    TimeSeries subTimeseries = new TimeSeries();
                    // copy meta info
                    subTimeseries.meta = copyMetricMeta(source.meta);
                    // copy datapoints for sub timeseries
                    source.data.stream()
                        .filter(datapoint -> datapoint.time >= localStart && datapoint.time < localEnd)
                        .forEach(datapoint -> {
                                try {
                                    subTimeseries.append(datapoint.time, datapoint.value);
                                } catch (Exception e) {
                                    log.error("Error while appending data to sub timeseries!");
                                }
                            });
                    return subTimeseries;
                })
                .filter(isValidTimeSeries())
                .map(timeSeries ->  EgadsUtils.fillMissingData(timeSeries, 1, 1))
                .collect(Collectors.toList());
            result[intervalIndex] = subTimeseriesList;
            intervalIndex += 1;
        }
        return result;
    }

    /**
     * Obtain a copy of a metric metadata object
     * with a new unique ID.
     *
     * @param source object to copy
     * @return a copy of the object with a new ID
     */
    public MetricMeta copyMetricMeta(MetricMeta source) {
        MetricMeta copy = new MetricMeta();
        copy.name = source.name;
        copy.source = source.source;
        copy.fileName = source.fileName;
        copy.id = UUID.randomUUID().toString();
        return copy;
    }

}
