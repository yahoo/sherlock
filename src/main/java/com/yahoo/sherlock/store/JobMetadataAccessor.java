/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.exception.JobNotFoundException;

import lombok.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The {@code JobMetadataAccessor} defines an interface for
 * communicating with a persistence layer, be it SQL, Redis, etc.,
 * to retrieve and store {@code JobMetadata} objects.
 */
public interface JobMetadataAccessor {

    /**
     * Get the {@code JobMetadata} object with the specified ID.
     *
     * @param jobId the ID for which to retrieve the job metadata
     * @return the job metadata with the specified ID
     * @throws IOException          if there is an error with the persistence layer
     * @throws JobNotFoundException if no job can be found with the specified ID
     */
    @NonNull
    JobMetadata getJobMetadata(String jobId) throws IOException, JobNotFoundException;

    /**
     * Put a {@code JobMetadata} object in the store.
     * This method will overwrite existing objects.
     * This method should generate a new ID for the job metadata
     * if one does not already exist.
     *
     * @param jobMetadata the job metadata to store
     * @return job id
     * @throws IOException if there is an error with the persistence layer
     */
    String putJobMetadata(JobMetadata jobMetadata) throws IOException;

    /**
     * Put a list of jobs in the store. This method should overwrite
     * all existing jobs and generate new IDs for jobs without them.
     * This is primarily used to perform bulk updates of jobs.
     *
     * @param jobs a list of jobs to add or update
     * @throws IOException if an error occurs while putting the jobs
     */
    void putJobMetadata(List<JobMetadata> jobs) throws IOException;

    /**
     * Delete a {@code JobMetadata} object from the store.
     *
     * @param jobId the ID for which to delete the job metadata
     * @throws IOException          if there is an error with the persistence layer
     * @throws JobNotFoundException if no job can be found with the specified ID
     */
    void deleteJobMetadata(String jobId) throws IOException, JobNotFoundException;

    /**
     * Wrapper for integer ID values.
     *
     * @param jobId job ID as an integer value
     * @throws IOException          if an error occurs during deletion
     * @throws JobNotFoundException if the job to be deleted does not exist
     */
    default void deleteJobMetadata(Integer jobId) throws IOException, JobNotFoundException {
        deleteJobMetadata(jobId.toString());
    }

    /**
     * Get a {@code List} of all {@code JobMetadata} objects in the store.
     *
     * @return a list of job metadata, which may be empty
     * @throws IOException if there is an error with the persistence layer
     */
    @NonNull
    List<JobMetadata> getJobMetadataList() throws IOException;

    /**
     * Get a {@code List} of {@code JobMetadata} objects whose status
     * is {@link com.yahoo.sherlock.enums.JobStatus#RUNNING RUNNING}.
     * This method is used to launch previously running jobs
     * when restarting the {@code App}.
     *
     * @return a list of jobs with the RUNNING status, which may be empty
     * @throws IOException if there is an error with the persistence layer
     */
    @NonNull
    List<JobMetadata> getRunningJobs() throws IOException;

    /**
     * Get a {@code List} of {@code JobMetadata} objects whose associated
     * {@link com.yahoo.sherlock.model.DruidCluster DruidCluster}
     * ID matches the specified ID. This method is used to ensure that no
     * clusters are deleted with active jobs.
     *
     * @param clusterId the cluster ID for which to find jobs
     * @return a list of job metadata that use the specified cluster, which may be empty
     * @throws IOException if there is an error with the persistence layer
     */
    @NonNull
    List<JobMetadata> getJobsAssociatedWithCluster(String clusterId) throws IOException;

    /**
     * Get a {@code List} of jobs that are associated with this cluster
     * and that are running. This method is used to retrieve cluster jobs
     * for rescheduling in case details of the cluster change.
     *
     * @param clusterId the cluster ID
     * @return a list of running jobs
     * @throws IOException if an error occurs
     */
    @NonNull
    List<JobMetadata> getRunningJobsAssociatedWithCluster(String clusterId) throws IOException;

    /**
     * Delete all jobs that are marked with 'DEBUG'.
     *
     * @throws IOException if an error occurs
     */
    void deleteDebugJobs() throws IOException;

    /**
     * Get the list of job metadata corresponding to a
     * set of job IDs.
     *
     * @param jobIds list of job IDs
     * @return list of corresponding jobs
     * @throws IOException if an error with the persistence layer occurs
     */
    @NonNull
    List<JobMetadata> getJobMetadata(Set<String> jobIds) throws IOException;

    /**
     * Get the set of job Ids.
     *
     * @return set of job Ids
     */
    Set<String> getJobIds();

    /**
     * Remove job Id from job index.
     *
     * @param jobId job Id
     */
    void removeFromJobIdIndex(String jobId);

    /**
     * Issue a bulk delete on a set of jobs. This method will add
     * all deleted jobs to the deleted jobs list.
     *
     * @param jobIds a set of job IDs to remove
     * @throws IOException if an error occurs
     */
    void deleteJobs(Set<String> jobIds) throws IOException;

    /**
     * Method to delete given email from all related jobs.
     * @param emailMetaData email metadata of emailId that is to be deleted
     * @throws IOException if an error occurs
     */
    void deleteEmailFromJobs(EmailMetaData emailMetaData) throws IOException;

    /**
     * Method to save redis snapshot.
     * @return OK
     */
    String saveRedisJobsMetadata() throws IOException;
}
