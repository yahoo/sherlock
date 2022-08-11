/*
 * Copyright 2022, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

/**
 * Exception thrown when there is an issue with the Detector Service.
 */
public class DetectorServiceException extends Exception {

    /**
     * Default constructor.
     */
    public DetectorServiceException() {
        super();
    }

    /**
     * Constructor with a message.
     *
     * @param message exception message
     */
    public DetectorServiceException(String message) {
        super(message);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public DetectorServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
