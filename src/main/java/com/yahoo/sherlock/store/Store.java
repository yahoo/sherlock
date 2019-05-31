/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.DatabaseConstants;

import com.yahoo.sherlock.store.redis.LettuceAnomalyReportAccessor;
import com.yahoo.sherlock.store.redis.LettuceDeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.redis.LettuceDruidClusterAccessor;
import com.yahoo.sherlock.store.redis.LettuceEmailMetadataAccessor;
import com.yahoo.sherlock.store.redis.LettuceJobMetadataAccessor;
import com.yahoo.sherlock.store.redis.LettuceJobScheduler;
import com.yahoo.sherlock.store.redis.LettuceJsonDumper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible for initializing and holding
 * the accessor instances for each data type that must be
 * stored in a persistent layer. The type of backend
 * database to use is specified in the
 * {@link CLISettings commandline settings}.
 */
@Slf4j
public class Store {

    /**
     * Enumeration of the different accessor types. Each of these
     * corresponds to one of the accessors interfaces.
     */
    public enum AccessorType {
        ANOMALY_REPORT,
        DELETED_JOB_METADATA,
        DRUID_CLUSTER,
        JOB_METADATA,
        EMAIL_METADATA,
        JSON_DUMPER,
        JOB_SCHEDULER
    }

    /**
     * Active anomaly report accessor instance.
     */
    private static AnomalyReportAccessor anomalyReportAccessor = null;
    /**
     * Active deleted job accessor instance.
     */
    private static DeletedJobMetadataAccessor deletedJobMetadataAccessor = null;
    /**
     * Active cluster accessor instance.
     */
    private static DruidClusterAccessor druidClusterAccessor = null;
    /**
     * Active job metadata accessor instance.
     */
    private static JobMetadataAccessor jobMetadataAccessor = null;
    /**
     * Active email metadata scheduler instance.
     */
    private static EmailMetadataAccessor emailMetadataAccessor = null;
    /**
     * Active json dumper instance.
     */
    private static JsonDumper jsonDumper = null;
    /**
     * Active job scheduler instance.
     */
    private static JobScheduler jobScheduler = null;

    /**
     * Build default parameters for the given backend type and accessor type.
     *
     * @param type accessor type
     * @return default parameters
     */
    public static StoreParams getParamsFor(AccessorType type) {
        StoreParams params = new StoreParams() {
            {
                put(DatabaseConstants.REDIS_HOSTNAME, CLISettings.REDIS_HOSTNAME);
                put(DatabaseConstants.REDIS_PORT, String.valueOf(CLISettings.REDIS_PORT));
                put(DatabaseConstants.REDIS_SSL, CLISettings.REDIS_SSL ? "true" : "false");
                put(DatabaseConstants.REDIS_TIMEOUT, String.valueOf(CLISettings.REDIS_TIMEOUT));
                put(DatabaseConstants.REDIS_PASSWORD, CLISettings.REDIS_PASSWORD);
                put(DatabaseConstants.REDIS_CLUSTERED, CLISettings.REDIS_CLUSTERED ? "true" : null);
                put(DatabaseConstants.INDEX_REPORT_JOB_ID, DatabaseConstants.INDEX_REPORT_JOB_ID);
                put(DatabaseConstants.INDEX_TIMESTAMP, DatabaseConstants.INDEX_TIMESTAMP);
                put(DatabaseConstants.INDEX_DELETED_ID, DatabaseConstants.INDEX_DELETED_ID);
                put(DatabaseConstants.INDEX_CLUSTER_ID, DatabaseConstants.INDEX_CLUSTER_ID);
                put(DatabaseConstants.INDEX_QUERY_ID, DatabaseConstants.INDEX_QUERY_ID);
                put(DatabaseConstants.INDEX_JOB_ID, DatabaseConstants.INDEX_JOB_ID);
                put(DatabaseConstants.INDEX_FREQUENCY, DatabaseConstants.INDEX_FREQUENCY);
                put(DatabaseConstants.INDEX_EMAILID_REPORT, DatabaseConstants.INDEX_EMAILID_REPORT);
                put(DatabaseConstants.INDEX_EMAILID_TRIGGER, DatabaseConstants.INDEX_EMAILID_TRIGGER);
                put(DatabaseConstants.INDEX_EMAILID_JOBID, DatabaseConstants.INDEX_EMAILID_JOBID);
                put(DatabaseConstants.INDEX_JOB_CLUSTER_ID, DatabaseConstants.INDEX_JOB_CLUSTER_ID);
                put(DatabaseConstants.INDEX_JOB_STATUS, DatabaseConstants.INDEX_JOB_STATUS);
                put(DatabaseConstants.QUEUE_JOB_SCHEDULE, DatabaseConstants.QUEUE_JOB_SCHEDULE);
                put(DatabaseConstants.INDEX_EMAIL_ID, DatabaseConstants.INDEX_EMAIL_ID);
            }
        };
        String dbName;
        String idName;
        switch (type) {
            case ANOMALY_REPORT:
                dbName = DatabaseConstants.REPORTS;
                idName = DatabaseConstants.REPORT_ID;
                break;
            case DELETED_JOB_METADATA:
                dbName = DatabaseConstants.DELETED_JOBS;
                idName = DatabaseConstants.DELETED_JOB_ID;
                break;
            case DRUID_CLUSTER:
                dbName = DatabaseConstants.DRUID_CLUSTERS;
                idName = DatabaseConstants.CLUSTER_ID;
                break;
            case EMAIL_METADATA:
                dbName = DatabaseConstants.EMAILS;
                idName = DatabaseConstants.EMAIL_ID;
                break;
            case JOB_METADATA:
            default:
                dbName = DatabaseConstants.JOBS;
                idName = DatabaseConstants.JOB_ID;
                break;
        }
        params.put(DatabaseConstants.DB_NAME, dbName);
        params.put(DatabaseConstants.ID_NAME, idName);
        return params;
    }

    /**
     * Initialize an accessor of the specified type using the backend
     * database type. This method should be called only once for
     * each accessor type.
     *
     * @param type the accessor type to create and initialize
     * @return the initialized accessor type
     */
    private static Object initializeAccessor(AccessorType type) {
        StoreParams params = getParamsFor(type);
        switch (type) {
            case ANOMALY_REPORT:
                return new LettuceAnomalyReportAccessor(params);
            case DELETED_JOB_METADATA:
                return new LettuceDeletedJobMetadataAccessor(params);
            case DRUID_CLUSTER:
                return new LettuceDruidClusterAccessor(params);
            case JOB_METADATA:
                return new LettuceJobMetadataAccessor(params);
            case JOB_SCHEDULER:
                return new LettuceJobScheduler(params);
            case JSON_DUMPER:
                return new LettuceJsonDumper(params);
            case EMAIL_METADATA:
                return new LettuceEmailMetadataAccessor(params);
            default:
                return null;
        }
    }

    /**
     * @return the anomaly report accessor instance
     */
    @NonNull
    public static AnomalyReportAccessor getAnomalyReportAccessor() {
        if (anomalyReportAccessor == null) {
            anomalyReportAccessor =
                    (AnomalyReportAccessor) initializeAccessor(AccessorType.ANOMALY_REPORT);
        }
        return anomalyReportAccessor;
    }

    /**
     * @return the deleted job metadata accessor instance
     */
    @NonNull
    public static DeletedJobMetadataAccessor getDeletedJobMetadataAccessor() {
        if (deletedJobMetadataAccessor == null) {
            deletedJobMetadataAccessor =
                    (DeletedJobMetadataAccessor) initializeAccessor(AccessorType.DELETED_JOB_METADATA);
        }
        return deletedJobMetadataAccessor;
    }

    /**
     * @return the druid cluster accessor instance
     */
    @NonNull
    public static DruidClusterAccessor getDruidClusterAccessor() {
        if (druidClusterAccessor == null) {
            druidClusterAccessor =
                    (DruidClusterAccessor) initializeAccessor(AccessorType.DRUID_CLUSTER);
        }
        return druidClusterAccessor;
    }

    /**
     * @return the job metadata accessor instance
     */
    @NonNull
    public static JobMetadataAccessor getJobMetadataAccessor() {
        if (jobMetadataAccessor == null) {
            jobMetadataAccessor =
                    (JobMetadataAccessor) initializeAccessor(AccessorType.JOB_METADATA);
        }
        return jobMetadataAccessor;
    }

    /**
     * @return the json dumper instance
     */
    @NonNull
    public static JsonDumper getJsonDumper() {
        if (jsonDumper == null) {
            jsonDumper =
                    (JsonDumper) initializeAccessor(AccessorType.JSON_DUMPER);
        }
        return jsonDumper;
    }

    /**
     * @return the job scheduler instance
     */
    @NonNull
    public static JobScheduler getJobScheduler() {
        if (jobScheduler == null) {
            jobScheduler =
                    (JobScheduler) initializeAccessor(AccessorType.JOB_SCHEDULER);
        }
        return jobScheduler;
    }

    /**
     * @return the email metadata accessor instance
     */
    public static EmailMetadataAccessor getEmailMetadataAccessor() {
        if (emailMetadataAccessor == null) {
            emailMetadataAccessor =
                    (EmailMetadataAccessor) initializeAccessor(AccessorType.EMAIL_METADATA);
        }
        return emailMetadataAccessor;
    }
}
