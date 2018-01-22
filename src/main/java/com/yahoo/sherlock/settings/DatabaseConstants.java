/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.settings;

/**
 * Constants for database names.
 */
public class DatabaseConstants {

    /**
     * Whether the Redis connection is to a cluster.
     */
    public static final String REDIS_CLUSTERED = "redisClustered";
    /**
     * The name of the Redis hostname parameter.
     */
    public static final String REDIS_HOSTNAME = "redisHostname";
    /**
     * The name of the Redis port parameter.
     */
    public static final String REDIS_PORT = "redisPort";
    /**
     * The name of the Redis SSL parameter.
     */
    public static final String REDIS_SSL = "redisSsl";
    /**
     * The name of the Redis password parameter.
     */
    public static final String REDIS_PASSWORD = "redisPassword";
    /**
     * The name of the Redis timeout parameter.
     */
    public static final String REDIS_TIMEOUT = "redisTimeout";
    /**
     * The name and value of the report job ID index parameter.
     */
    public static final String INDEX_REPORT_JOB_ID = "reportJobIdIndex";
    /**
     * The name and value of the report timestamp index parameter.
     */
    public static final String INDEX_TIMESTAMP = "timestampIndex";
    /**
     * The name and value of the job frequency index.
     */
    public static final String INDEX_FREQUENCY = "frequencyIndex";
    /**
     * The name and value of the deleted job ID index parameter.
     */
    public static final String INDEX_DELETED_ID = "deletedIdIndex";
    /**
     * The name and value of the cluster ID index parameter.
     */
    public static final String INDEX_CLUSTER_ID = "clusterIdIndex";
    /**
     * The name and value of the instant report query ID index parameter.
     */
    public static final String INDEX_QUERY_ID = "queryIdIndex";
    /**
     * The name and value of the job ID index parameter.
     */
    public static final String INDEX_JOB_ID = "jobIdIndex";
    /**
     * The name and value of the job cluster ID index parameter.
     */
    public static final String INDEX_JOB_CLUSTER_ID = "jobClusterIdIndex";
    /**
     * The name and value of the job status ID index parameter.
     */
    public static final String INDEX_JOB_STATUS = "jobStatusIndex";
    /**
     * The name of the anomaly report field of anomaly timestamps.
     */
    public static final String ANOMALY_TIMESTAMP = "anomalyTimestamps";
    /**
     * The name of the job queue parameter.
     */
    public static final String QUEUE_JOB_SCHEDULE = "jobQueue";

    /**
     * The name of the database name parameter.
     */
    public static final String DB_NAME = "dbName";
    /**
     * The name of the ID generator name parameter.
     */
    public static final String ID_NAME = "idName";

    /**
     * Database name for instant anomaly report.
     */
    public static final String ANOMALIES = "Anomalies";
    /**
     * Database name for storing jobs info.
     */
    public static final String JOBS = "Jobs";
    /**
     * Database name for Druid clusters.
     */
    public static final String DRUID_CLUSTERS = "DruidClusters";
    /**
     * Database name for storing deleted jobs info.
     */
    public static final String DELETED_JOBS = "DeletedJobs";
    /**
     * Database name for storing anomaly reports.
     */
    public static final String REPORTS = "Reports";
    /**
     * Database name for storing Headers(schema) for Serializers.
     */
    public static final String HEADERS = "Headers";
    /**
     * Atomic jobId generator name.
     */
    public static final String JOB_ID = "JobId";
    /**
     * Atomic queryId generator name.
     */
    public static final String QUERY_ID = "QueryId";
    /**
     * Atomic clusterId generator name.
     */
    public static final String CLUSTER_ID = "ClusterId";
    /**
     * Atomic reportId generator name.
     */
    public static final String REPORT_ID = "ReportId";
    /**
     * Atomic deletedJobId generator name.
     */
    public static final String DELETED_JOB_ID = "DeletedJobId";

}
