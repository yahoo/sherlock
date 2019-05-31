/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.google.gson.JsonObject;
import com.yahoo.sherlock.settings.DatabaseConstants;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.util.List;

/**
 * This class is responsible for grabbing the entire backend
 * as a JSON dump or by updating a backend with a Json Object.
 */
public interface JsonDumper {

    String[] INDEX_NAMES = {
        DatabaseConstants.INDEX_REPORT_JOB_ID,
        DatabaseConstants.INDEX_TIMESTAMP,
        DatabaseConstants.INDEX_DELETED_ID,
        DatabaseConstants.INDEX_CLUSTER_ID,
        DatabaseConstants.INDEX_QUERY_ID,
        DatabaseConstants.INDEX_JOB_CLUSTER_ID,
        DatabaseConstants.INDEX_JOB_ID,
        DatabaseConstants.INDEX_JOB_STATUS,
        DatabaseConstants.INDEX_FREQUENCY,
        DatabaseConstants.INDEX_EMAILID_REPORT,
        DatabaseConstants.INDEX_EMAIL_ID,
        DatabaseConstants.INDEX_EMAILID_TRIGGER,
        DatabaseConstants.INDEX_EMAILID_JOBID
    };

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

}
