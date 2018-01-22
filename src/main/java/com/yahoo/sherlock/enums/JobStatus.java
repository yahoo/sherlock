/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import lombok.extern.slf4j.Slf4j;

/**
 * Enum type specifying job status.
 */
@Slf4j
public enum JobStatus {

    CREATED("CREATED"),       // When user creates a new job
    RUNNING("RUNNING"),       // When the job is running (after launch)
    STOPPED("STOPPED"),       // When the job is stopped (by user or due to some error)
    ERROR("ERROR"),           // When the exception causes during the execution
    NODATA("NODATA"),         // When druid response has no data or incomplete data
    ZOMBIE("ZOMBIE");         // When the job has unexpected scheduling behavior

    /**
     * String status value for the job.
     * Value uniquely identifies the job status.
     */
    private final String value;

    /**
     * Name of the status.
     */
    private final String name;

    /**
     * Constructor sets the value of the job status
     * and sets the name to the lowercase string value
     * of the enum.
     *
     * @param value string status value
     */
    JobStatus(String value) {
        this.value = value;
        this.name = name().toLowerCase();
    }

    /**
     * @return value the string value of the job status
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
