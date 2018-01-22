/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

/**
 * Exception thrown when an error related to Druid occurs.
 */
public class DruidException extends Exception {

    /**
     * Default constructor.
     */
    public DruidException() {
        super();
    }

    /**
     * Constructor with a message.
     * @param message exception message
     */
    public DruidException(String message) {
        super(message);
    }

    /**
     * Construction with a message and throwable.
     * @param message exception message
     * @param cause cause of exception
     */
    public DruidException(String message, Throwable cause) {
        super(message, cause);
    }

}
