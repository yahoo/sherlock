/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.model.AnomalyReport;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

/**
 * The {@code AnomalyReportAccessor} defines an interface for
 * communicating with a persistence layer, to retrieve and store
 * {@code AnomalyReport} objects. The implementing class must
 * provide a constructor that accepts the accessor configuration.
 */
public interface AnomalyReportAccessor {

    /**
     * Put an anomaly report in the database keyed on the
     * report ID. If the ID of the incoming report is null,
     * a new ID should be generated and the new report is to be
     * inserted. If the ID already exists, then the previous
     * report should be overridden. Usually, the ID should
     * have already been set based on the time series metric.
     * @param reports the anomaly report to insert
     * @param emailIds list of EmailIDs to index the report ID in order to send it later to user
     * @throws IOException if an error occurs
     */
    void putAnomalyReports(List<AnomalyReport> reports, List<String> emailIds) throws IOException;

    /**
     * Get a list of anomaly reports that have the specified job ID.
     * This method should search the database for all anomaly reports
     * whose job ID parameter match the given argument.
     * @param jobId the job ID for which to find reports
     * @param frequency frequency of the job
     * @return a list of associated reports, which may be empty
     * @throws IOException if an error occurs
     */
    @NonNull
    List<AnomalyReport> getAnomalyReportsForJob(String jobId, String frequency) throws IOException;

    /**
     * Get a list of anomaly reports that are present in given emailId Index.
     * This method should search the database for all anomaly reports
     * present in the index of given emailId.
     * @param emailId the emailId for which to find reports
     * @return a list of associated reports, which may be empty
     * @throws IOException if an error occurs
     */
    @NonNull
    List<AnomalyReport> getAnomalyReportsForEmailId(String emailId) throws IOException;

    /**
     * Get a list of anomaly reports that have the specified job ID
     * and that occurred at the given time. This method should
     * search the database for all anomaly reports
     * whose job ID and generation time parameters match the given arguments.
     * @param jobId the job ID for which to find reports
     * @param time the time during which to find reports
     * @param frequency frequency of the job
     * @return a list of matching reports, which may be empty
     * @throws IOException if an error occurs
     */
    @NonNull
    List<AnomalyReport> getAnomalyReportsForJobAtTime(String jobId, String time, String frequency) throws IOException;

    /**
     * Delete all anomaly reports for a particular job as
     * specified by the job ID. This method should lookup
     * the job ID index for the reports, clear that index
     * and delete each report in the index. This method
     * should also clear the report IDs from their respective
     * timestamp indices.
     * @param jobId job ID for which to delete all associated reports
     * @throws IOException if an error occurs during deletion
     */
    void deleteAnomalyReportsForJob(String jobId) throws IOException;

    /**
     * Delete all anomaly reports for a particular job at
     * specified timestamp. This method should
     * search the database for all anomaly reports
     * whose job ID and generation time parameters match the given arguments, clear that index
     * and delete each report in the index. This should delete the report IDs
     * from their respective timestamp indices.
     * @param jobId job ID for which to delete all associated reports
     * @param time the time during which to find reports
     * @param frequency frequency of the job
     * @throws IOException if an error occurs during deletion
     */
    void deleteAnomalyReportsForJobAtTime(String jobId, String time, String frequency) throws IOException;
}
