/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.google.gson.JsonObject;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.util.List;

/**
 * This class is responsible for grabbing the entire backend
 * as a JSON dump or by updating a backend with a Json Object.
 */
public interface JsonDumper {

    /**
     * Method to return pending jobs in the queue in redis.
     * @return a JSON object
     * @throws IOException if an error reading the backend occurs
     */
    List<ImmutablePair<String, String>> getQueuedJobs() throws IOException;

    /**
     * Returns the raw data of the backend as it is stored
     * as a JSON object.
     * @return a JSON object
     * @throws IOException if an error reading the backend occurs
     */
    JsonObject getRawData() throws IOException;

    /**
     * Update the backend with the data stored in the provided
     * JSON object. This method does not flush the backend
     * before writing.
     * @param json the JSON object to write
     * @throws IOException if an error writing to the backend occurs
     */
    void writeRawData(JsonObject json) throws IOException;

    /**
     * Clear the value of given index.
     * @param index index names
     * @param id id for the index
     */
    void clearIndexes(String index, String id);

}
