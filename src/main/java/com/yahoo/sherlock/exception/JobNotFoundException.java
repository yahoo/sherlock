/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

/**
 * Exception thrown when no {@code JobMetadata} can be
 * found with a specified ID.
 */
public class JobNotFoundException extends Exception {

    /**
     * Default constructor.
     */
    public JobNotFoundException() {
        super();
    }

    /**
     * Constructor with a message.
     *
     * @param message exception message
     */
    public JobNotFoundException(String message) {
        super(message);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public JobNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
