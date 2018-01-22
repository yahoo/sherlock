/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.sherlock.utils.Utils;

import lombok.Data;
import spark.QueryParamsMap;

/**
 * Deserializer class for user query request.
 */
@Data
public class UserQuery {

    /**
     * Build a {@code UserQuery} object from {@code Spark Request}
     * query parameter map.
     *
     * @param params parameter map
     * @return a user query instance
     */
    public static UserQuery fromQueryParams(QueryParamsMap params) {
        return Utils.deserializeQueryParams(new UserQuery(), params);
    }

    /** Input user query. */
    private String query;

    /** Test name entered by user. */
    private String testName;

    /** Description of the test. */
    private String testDescription;

    /** Url of superset associated with the query. */
    private String queryUrl;

    /** Owner of the anomaly test. */
    private String owner;

    /** Email id of the owner. */
    private String ownerEmail;

    /** Granularity of data. */
    private String granularity;

    /** Frequency of the job. */
    private String frequency;

    /** Threshold for standard deviation on normal distribution curve. */
    private Double sigmaThreshold;

    /** URL to send instant anomaly request. */
    private String druidUrl;

    /** Id of the associated cluster for this job. */
    private Integer clusterId;
}
