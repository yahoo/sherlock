/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

/**
 * Exception thrown when there is an
 * error instantiating accessors.
 */
public class StoreException extends RuntimeException {

    /**
     * Default constructor.
     */
    public StoreException() {
        super();
    }

    /**
     * Constructor with a message.
     *
     * @param message exception message
     */
    public StoreException(String message) {
        super(message);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public StoreException(String message, Throwable cause) {
        super(message, cause);
    }

}
