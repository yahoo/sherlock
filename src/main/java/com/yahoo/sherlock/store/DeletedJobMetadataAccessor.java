/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.model.JobMetadata;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

/**
 * This class provides an interface for storing and
 * retrieving {@code JobMetadata} objects that have
 * been deleted from the regular store, so that a record
 * of all past jobs can be maintained. The implementing class must
 * provide a constructor that accepts the accessor configuration.
 */
public interface DeletedJobMetadataAccessor {

    /**
     * Put a job metadata object in the database that has been deleted
     * from the active job database. All job metadata objects passed
     * to this method should already have an ID assigned. Job metadata
     * objects are passed here when they are deleted by the
     * {@link JobMetadataAccessor job accessor}.
     * @param jobMetadata the job metadata to insert
     * @throws IOException if an error occurs with insertion
     */
    void putDeletedJobMetadata(JobMetadata jobMetadata) throws IOException;

    /**
     * Get a job metadata with a specific job ID. This method
     * is called to grab the deleted job metadata when a user
     * clicks on a deleted job in the list.
     *
     * @param jobId the ID for which to retrieve the job
     * @return the job metadata object corresponding to the ID
     * @throws IOException          if an error with the database occurs
     * @throws JobNotFoundException if no job can be found with the specified ID
     */
    @NonNull
    JobMetadata getDeletedJobMetadata(String jobId) throws IOException, JobNotFoundException;

    /**
     * Get the entire list of deleted job metadata objects.
     * This method is used to display the deleted jobs list on the
     * front end.
     *
     * @return a list of jobs, which may be empty
     * @throws IOException if an error occurs
     */
    @NonNull
    List<JobMetadata> getDeletedJobMetadataList() throws IOException;

    /**
     * Insert a list of deleted jobs. This is used when a bulk
     * delete is used on existing jobs.
     *
     * @param jobs a list of deleted jobs to insert
     * @throws IOException if an error occurs
     */
    void putDeletedJobMetadata(List<JobMetadata> jobs) throws IOException;

}
