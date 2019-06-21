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
