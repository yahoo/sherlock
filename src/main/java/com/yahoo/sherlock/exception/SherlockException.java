/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

/**
 * General exception type for Sherlock.
 */
public class SherlockException extends Exception {

    /**
     * Default constructor.
     */
    public SherlockException() {
        super();
    }

    /**
     * Constructor with a message.
     *
     * @param message exception message
     */
    public SherlockException(String message) {
        super(message);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public SherlockException(String message, Throwable cause) {
        super(message, cause);
    }

}
