/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.exception;

/**
 * Exception thrown upon an error with the job
 * scheduling and execution services.
 */
public class SchedulerException extends Exception {

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }

}
