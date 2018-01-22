/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.google.gson.JsonElement;

import lombok.Data;

/**
 * Deserializer class for druid response json datapoints.
 */
@Data
public class JsonDataPoint {

    /** Deserializer for 'result' blob in json. */
    private JsonElement result;

    /** Deserializer for 'event' blob in json. */
    private JsonElement event;

    /** Deserializer for 'timestamp'. */
    private String timestamp;
}
