/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.query;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.exception.LambdaException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.JsonDataPoint;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Deserializer class for Druid response {@code JsonArray}.
 */
@Slf4j
public class JsonTimeSeries {

    /**
     * Deserializer class for JSON data points in Druid response.
     */
    public static class JsonDataSequence extends ArrayList<JsonDataPoint> {
    }

    /**
     * List of Json datapoints.
     */
    private JsonDataSequence jsonDataSequence;

    /**
     * Set to store query groupby dimensions.
     */
    private LinkedHashSet<String> dimensions;

    /**
     * Set to store metrics names.
     */
    private Set<String> metrics;

    /**
     * Map to store Unique-timeseries-name to UUID mapping.
     */
    private Map<String, UUID> uniqueIDMap;

    /**
     * Map to store UUID to Unique-timeseries mapping.
     */
    private Map<UUID, TimeSeries> uniqueTimeSeriesMap;

    /**
     * Dateformat of Druid response timestamp.
     */
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * Getter for JSON data sequence.
     *
     * @return jsonDataSequence
     */
    public JsonDataSequence getJsonDataSequence() {
        return jsonDataSequence;
    }

    /**
     * Getter for unique timeseries map.
     *
     * @return uniqueTimeSeriesMap
     */
    public Map<UUID, TimeSeries> getUniqueTimeSeriesMap() {
        return uniqueTimeSeriesMap;
    }

    /**
     * Constructor to deserialize JSON array to JSON data sequence and intialize query metadata.
     *
     * @param jsonArray Druid response
     * @param query     associated query
     */
    public JsonTimeSeries(JsonArray jsonArray, Query query) {
        this.metrics = query.getMetricNames();
        this.dimensions = query.getGroupByDimensions();
        this.jsonDataSequence = new Gson().fromJson(jsonArray, JsonDataSequence.class);
        this.uniqueIDMap = new HashMap<>();
        this.uniqueTimeSeriesMap = new HashMap<>();
    }

    /**
     * Method to generate new UUID for new timeseries.
     *
     * @param uniqueTimeSeriesName new timeseries name
     * @return UUID
     */
    private UUID getNewUUID(String uniqueTimeSeriesName) {
        UUID uuid = UUID.randomUUID();
        uniqueIDMap.put(uniqueTimeSeriesName, uuid);
        return uuid;
    }

    /**
     * Method to initialize and return new timeseries.
     *
     * @param uuid            uuid to be used as an unique id for new timeseries
     * @param metricName      metric name associated with the new timeseries
     * @param dimensionValues group by dimensions involved in new timeseries data
     * @return a new time series with the given parameters
     */
    private TimeSeries getNewTimeSeries(UUID uuid, String metricName, String dimensionValues) {
        TimeSeries timeSeries = new TimeSeries();
        timeSeries.meta.name = metricName;
        timeSeries.meta.id = uuid.toString();
        timeSeries.meta.source = dimensionValues;
        timeSeries.meta.fileName = metricName;
        uniqueTimeSeriesMap.put(uuid, timeSeries);
        return timeSeries;
    }

    /**
     * Predicate for null datapoint object filter.
     *
     * @return true if object is non-null else false
     */
    public static Predicate<JsonDataPoint> isNonNull() {
        return Objects::nonNull;
    }

    /**
     * Predicate to filter incomplete data object.
     *
     * @return true if all necessary fields are present else false
     */
    public static Predicate<JsonDataPoint> isComplete() {
        return p -> (p.getTimestamp() != null && (p.getResult() != null || p.getEvent() != null));
    }

    /**
     * Combined all filteres in one predicate.
     *
     * @return result of ANDing two filters
     */
    public static Predicate<JsonDataPoint> combinedFilters() {
        return isNonNull().and(isComplete());
    }

    /**
     * Null valued dimension filter.
     *
     * @param jsonElement JSON blob of datapoints by dimension values.
     * @return true if dimensional value is 'null' else false
     */
    public Predicate<String> isNullDimensionJsonBlob(JsonElement jsonElement) {
        return dim -> jsonElement.getAsJsonObject().get(dim).isJsonNull();
    }

    /**
     * Method to parse Druid timestamp.
     *
     * @param timestamp input format 'yyyy-MM-ddTHH:mm:ss.SSSZ'
     * @return timestamp in seconds
     */
    private Long parseTimeStamp(String timestamp) throws SherlockException {
        if (timestamp != null) {
            timestamp = timestamp.replace("T", " ").replace("Z", "");
            DateFormat df = new SimpleDateFormat(DATE_FORMAT);
            df.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date parsedDate;
            try {
                parsedDate = df.parse(timestamp);
            } catch (ParseException e) {
                log.error("Druid timestamp parsing error!", e);
                throw new SherlockException(e.getMessage(), e);
            }
            Timestamp tp = new Timestamp(parsedDate.getTime());
            return (tp.getTime() / 1000);
        } else {
            log.info("Found null timestamp in Druid response");
            throw new SherlockException("Null Timestamp in Druid response");
        }
    }

    /**
     * Method to generate unique name for timeseries based
     * on values of groupby dimensions.
     *
     * @param blob Json representing a datapoint
     * @return unique name of timeseires(ex. "news_mobile","news_desktop")
     */
    private String getGroupByDimensionValues(JsonElement blob) {
        return dimensions.stream().anyMatch(isNullDimensionJsonBlob(blob))
                ? "null"
                : dimensions.stream()
                .map(value -> value + " = '" + blob.getAsJsonObject().get(value).getAsString() + "'")
                .collect(Collectors.joining("\n"));
    }

    /**
     * Method to get the metric values from JSON datapoint and put
     * them in different timeseries accordingly.
     *
     * @param parsedTimeStamp parsed timestamp in seconds format
     * @param blob            Json containing datapoint info
     */
    private void processJsonBlob(Long parsedTimeStamp, JsonElement blob) throws SherlockException {
        // Get the groupby dimension values comma(,) separated
        String dimensionValues = (dimensions.size() == 0) ? "" : getGroupByDimensionValues(blob);
        // Check for 'null' as a dimesional value
        if (!dimensionValues.contains("null")) {
            for (String metricName : metrics) {
                if (metricName == null) {
                    continue;
                }
                String uniqueTimeSeriesName = metricName + "|" + dimensionValues;
                UUID uuid = uniqueIDMap.containsKey(uniqueTimeSeriesName)
                        ? uniqueIDMap.get(uniqueTimeSeriesName)
                        : getNewUUID(uniqueTimeSeriesName);
                TimeSeries timeSeries = uniqueTimeSeriesMap.containsKey(uuid)
                        ? uniqueTimeSeriesMap.get(uuid)
                        : getNewTimeSeries(uuid, metricName, dimensionValues);
                try {
                    timeSeries.append(parsedTimeStamp, blob.getAsJsonObject().get(metricName).getAsFloat());
                } catch (Exception e) {
                    log.error("Error while populating the time series!", e);
                    throw new SherlockException(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Method to parse 'result' JSON object (it can be a JSON array or JSON object with JSON primitives).
     * This case occures when there is one groupby dimension or none.
     *
     * @param parsedTimeStamp parsed timestamp in seconds format
     * @param jsonDataPoint   JSON datapoint containing timestamp and result
     * @throws SherlockException if an error occurs during parsing
     */
    private void parseResult(Long parsedTimeStamp, JsonDataPoint jsonDataPoint) throws SherlockException {
        // Get the result field as a JSON element
        JsonElement result = jsonDataPoint.getResult();
        // If result is an array
        if (result != null && result.isJsonArray() && result.getAsJsonArray().size() > 0) {
            try (Stream<JsonElement> eventStream = StreamSupport.stream(result.getAsJsonArray().spliterator(), true)) {
                // For each JSON blob in the array call processJsonBlob method
                eventStream.forEach(
                    LambdaException.consumerExceptionHandler(blob -> processJsonBlob(parsedTimeStamp, blob)));
            } catch (Exception e) {
                log.error("Exception caught while iterating though JSON blob array!", e);
                throw new SherlockException(e.getMessage(), e);
            }
            // If result is a JSON object
        } else if (result != null && result.isJsonObject() && result.getAsJsonObject().size() > 0) {
            // Call processJsonBlob method
            processJsonBlob(parsedTimeStamp, result);
        } else {
            log.error("Error in parsing, result is empty!");
        }
    }

    /**
     * Method to parse 'event' JSON object (it is always a JSON object with JSON primitives).
     * This case occures when there is more than one groupby dimensions.
     *
     * @param parsedTimeStamp parsed timestamp in seconds format
     * @param jsonDataPoint   JSON datapoint containing timestamp and event
     */
    private void parseEvent(Long parsedTimeStamp, JsonDataPoint jsonDataPoint) throws SherlockException {
        // Get the event field as a JSON element
        JsonElement event = jsonDataPoint.getEvent();
        // Check for JSON object
        if (event != null && event.isJsonObject() && event.getAsJsonObject().size() > 0) {
            processJsonBlob(parsedTimeStamp, event);
        } else {
            throw new SherlockException("Error in Druid response parsing!");
        }
    }

    /**
     * Method to process JSON formatted datapoints.
     *
     * @param jsonDataPoint JSON datapoint containing timestamp and result/event
     * @throws SherlockException if an error occurs in parsing the 'result' JSON
     */
    public void processJsonDataPoint(JsonDataPoint jsonDataPoint) throws SherlockException {
        if (jsonDataPoint != null) {
            Long parsedTimeStamp = parseTimeStamp(jsonDataPoint.getTimestamp());
            if (jsonDataPoint.getResult() != null) {
                parseResult(parsedTimeStamp, jsonDataPoint);
            } else {
                parseEvent(parsedTimeStamp, jsonDataPoint);
            }
        } else {
            throw new SherlockException("Null datapoint in Druid response");
        }
    }
}
