/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

/**
 * Exception thrown when no {@code DruidCluster} can be
 * found with a specified ID.
 */
public class ClusterNotFoundException extends Exception {

    /** Exception message. */
    public static final String EXEPTION_MSG = "Cluster is not available";

    /**
     * Default constructor.
     */
    public ClusterNotFoundException() {
        super();
    }

    /**
     * Constructor with a message.
     *
     * @param message exception message
     */
    public ClusterNotFoundException(String message) {
        super(EXEPTION_MSG);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public ClusterNotFoundException(String message, Throwable cause) {
        super(EXEPTION_MSG, cause);
    }

}
