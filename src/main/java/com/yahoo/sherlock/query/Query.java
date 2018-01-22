/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.query;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.settings.QueryConstants;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class to store new query json object and helper methods to operate on query.
 */
@Slf4j
@Data
public class Query {

    /**
     * JsonObject representing the druid query.
     */
    private JsonObject queryObj;

    /**
     * The relative time of the query's execution in
     * seconds since epoch.
     */
    private Integer runtime;

    /**
     * Granularity of the query.
     */
    private Granularity granularity;

    /**
     * Construct a query with a JsonObject representation
     * and a runtime.
     *
     * @param query       query as json Object
     * @param runtime     runtime in seconds as a string
     * @param granularity the granularity of the query
     */
    public Query(JsonObject query, Integer runtime, Granularity granularity) {
        this.queryObj = query;
        this.runtime = runtime;
        this.granularity = granularity;
    }

    /**
     * @return the container query as a JSON object
     */
    public JsonObject getQueryJsonObject() {
        return queryObj;
    }

    /**
     * @return a set a all metric names in the Druid query
     */
    @NonNull
    public Set<String> getMetricNames() {
        if (queryObj == null) {
            return Collections.emptySet();
        }
        // If there are post aggregations only consider post aggregations
        Set<String> metrics = new TreeSet<>();
        JsonElement postaggsEle = queryObj.get(QueryConstants.POSTAGGREGATIONS);
        JsonElement aggsEle = queryObj.get(QueryConstants.AGGREGATIONS);
        JsonArray postaggs;
        JsonArray aggs;
        if (postaggsEle != null && postaggsEle.isJsonArray()
                && (postaggs = postaggsEle.getAsJsonArray()).size() > 0) {
            for (JsonElement postaggEle : postaggs) {
                if (!postaggEle.isJsonObject()) {
                    continue;
                }
                JsonObject postagg = postaggEle.getAsJsonObject();
                JsonElement postaggNameEle = postagg.get(QueryConstants.NAME);
                JsonPrimitive postAggName;
                if (postaggNameEle != null && postaggNameEle.isJsonPrimitive()
                        && (postAggName = postaggNameEle.getAsJsonPrimitive()).isString()) {
                    metrics.add(postAggName.getAsString());
                }
            }
        } else if (aggsEle != null && aggsEle.isJsonArray()
                && (aggs = aggsEle.getAsJsonArray()).size() > 0) {
            for (JsonElement aggEle : aggs) {
                if (!aggEle.isJsonObject()) {
                    continue;
                }
                JsonObject agg = aggEle.getAsJsonObject();
                // If `name` is not found, try to find the name in `aggregator`
                JsonElement aggNameEle = agg.get(QueryConstants.NAME);
                JsonPrimitive aggName;
                if ((aggNameEle != null ||
                        ((aggNameEle = agg.get(QueryConstants.AGGREGATOR)) != null
                                && aggNameEle.isJsonObject()
                                && (aggNameEle = aggNameEle.getAsJsonObject().get(QueryConstants.NAME)) != null))
                        && aggNameEle.isJsonPrimitive()
                        && (aggName = aggNameEle.getAsJsonPrimitive()).isString()) {
                    metrics.add(aggName.getAsString());
                }
            }
        }
        return metrics;
    }

    /**
     * Method to get group by dimensions from query.
     * Dimensions can be a json primitive or a json array
     *
     * @return linked hash set of dimensions(Linked set to maintain the order while processing each datapoints)
     */
    public LinkedHashSet<String> getGroupByDimensions() {
        if (this.queryObj == null) {
            return null;
        }
        LinkedHashSet<String> dimensions = new LinkedHashSet<>();
        // check for multiple dimensions
        if (this.queryObj.has(QueryConstants.DIMENSIONS) && this.queryObj.get(QueryConstants.DIMENSIONS).isJsonArray()) {
            JsonArray dimensionsArray = this.queryObj.getAsJsonArray(QueryConstants.DIMENSIONS);              // get dimensions as an array
            dimensionsArray.forEach(jsonElement -> dimensions.add(jsonElement.getAsString()));        // make a set of dimensions
        } else if (this.queryObj.has(QueryConstants.DIMENSION)) {                                             // check for single dimension
            dimensions.add(this.queryObj.getAsJsonPrimitive(QueryConstants.DIMENSION).getAsString());
        }
        return dimensions;
    }

    /**
     * Method to get the datasource from query.
     *
     * @return the datasource of the query as a JsonElement
     */
    public JsonElement getDatasource() {
        if (this.queryObj == null) {
            return null;
        }
        // Check if the datasource key exists
        if (this.queryObj.has(QueryConstants.DATASOURCE)) {
            return this.queryObj.get(QueryConstants.DATASOURCE);
        }
        return null;
    }
}
