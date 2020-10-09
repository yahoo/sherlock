/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

/**
 * Exception Handler interface for ScheduledExceptionHandler.
 */
public interface ScheduledExceptionHandler {

    /**
     * Exception handling method.
     * @param e throwable object
     * @return true/false
     */
    boolean exceptionOccurred(Throwable e);
}
