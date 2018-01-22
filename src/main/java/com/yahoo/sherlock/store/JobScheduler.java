/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.store;

import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.exception.JobNotFoundException;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * This class is responsible for managing and communicating
 * with the backend task priority queue.
 */
public interface JobScheduler {

    /**
     * Schedule a job in the priority queue. The job ID is stored
     * ordered by its timestamp in minutes. This method should
     * insert the job as an atomic operation.
     *
     * @param timestampMinutes the job run time in minutes since epoch
     * @param jobId            the job ID to run
     * @throws IOException if an error occurs while scheduling the job
     */
    void pushQueue(long timestampMinutes, String jobId) throws IOException;

    /**
     * Wrapper for integer job ID value.
     *
     * @param timestampMinutes timestamp in minutes for job execution
     * @param jobId            job ID integer value
     * @throws IOException if an error occurs while scheduling the job
     */
    default void pushQueue(long timestampMinutes, Integer jobId) throws IOException {
        pushQueue(timestampMinutes, jobId.toString());
    }

    /**
     * Schedule a list of jobs in the queue, given a list of pairs
     * of job runtimes and job IDs.
     *
     * @param jobsAndTimes a list of time and job IDs pairs
     * @throws IOException if an error occurs while scheduling the jobs
     */
    void pushQueue(List<Pair<Integer, String>> jobsAndTimes) throws IOException;

    /**
     * Remove a job from the priority queue, so that it will
     * not longer be executed. This method shoul be an atomic operation.
     *
     * @param jobId the job ID to remove
     * @throws IOException          if an error occurs while unscheduling the job
     * @throws JobNotFoundException if the job was never scheduled
     */
    void removeQueue(String jobId) throws IOException, JobNotFoundException;

    /**
     * Wrapper method for integer job ID values.
     *
     * @param jobId job ID as an integer
     * @throws IOException          if an error occurs while unscheduling the job
     * @throws JobNotFoundException if the job was never scheduled
     */
    default void removeQueue(Integer jobId) throws IOException, JobNotFoundException {
        removeQueue(jobId.toString());
    }

    /**
     * Remove a set of job IDs from the queue, so that they are
     * no longer executed. This method is typically used to reschedule
     * bulk jobs.
     *
     * @param jobIds a set of job IDs to unschedule
     * @throws IOException if an error occurs
     */
    void removeQueue(Collection<String> jobIds) throws IOException;

    /**
     * Remove all jobs from the queue. This operation
     * should be atomic.
     *
     * @throws IOException if an error occurs while removing the queue
     */
    void removeAllQueue() throws IOException;

    /**
     * Get an ordered list of all job IDs in the queue sorted
     * from highest timestamp to lowest timestamp.
     *
     * @return an list of schedule job IDs
     * @throws IOException if an error occurs while retrieving the list
     */
    List<JobMetadata> getAllQueue() throws IOException;

    /**
     * Get the number of jobs in the priority queue whose
     * execution time is equal to or less than the provided
     * timestamp in minutes. In other words, this method
     * returns the number of jobs that should have been
     * executed by the provided time.
     *
     * @param timestampMinutes the timestamp in minutes
     * @return the number of jobs to execute
     * @throws IOException if an error occurs during peeking
     */
    int peekQueue(long timestampMinutes) throws IOException;

    /**
     * Pop the next job from the queue whose execution time
     * is equal to or less than the provided time. This method
     * returns null if there is no such job.
     *
     * @param timestampMinutes the current time in minutes
     * @return the next job to execute or null
     * @throws IOException if an error occurs while getting the next job
     */
    JobMetadata popQueue(long timestampMinutes) throws IOException;

    /**
     * When jobs are popped from the job queue, they may be added
     * to a pending queue in case a job runner fails. This method
     * will call the backend to remove a successfully completed
     * job from the pending queue.
     *
     * @param jobId the job id that has completed
     * @throws IOException if an error removing from the queue occurs
     */
    void removePending(String jobId) throws IOException;

    /**
     * Wrapper for integer job ID value.
     *
     * @param jobId ID as an integer value
     * @throws IOException if an error occurs while removing from the queue
     */
    default void removePending(Integer jobId) throws IOException {
        removePending(jobId.toString());
    }
}
