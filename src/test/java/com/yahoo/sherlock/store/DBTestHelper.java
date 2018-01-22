/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.JobMetadata;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper test class for mapdb testcases.
 */
@Slf4j
public class DBTestHelper {

    public static final String TEST_DB = "src/test/resources/test_database_1.db";
    public static final Integer JOB_ID = 1001;
    public static final String UNIQUE_ID = "abc10022";
    public static final Integer REPORT_NOMINAL_TIME = 1508348470 / 60;
    public static final String JOB_OWNER = "abc";
    public static final String JOB_OWNER_EMAIL = "abc@email.com";
    public static final String USER_QUERY = "{}";
    public static final String QUERY = "{}";
    public static final String TEST_NAME = "testname";
    public static final String TEST_DESCRIPTION = "test-desc";
    public static final String URL = "http://xyz.com";
    public static final String JOB_STATUS = JobStatus.CREATED.getValue();
    public static final String JOB_FREQUENCY = Triggers.DAY.toString();
    public static final String JOB_GRANULARITY = Granularity.DAY.toString();
    public static final Double SIGMA_VALUE = 3.0;
    public static final Integer JOB_NEXT_RUN_TIME = 2234;
    public static final Integer CLUSTER_SLA = 0;
    public static final Integer CLUSTER_ID = 1234;
    public static final String CLUSTER_NAME = "sherlock";
    public static final String CLUSTER_DESCRIPTION = "main-cluster";
    public static final String BROKER_HOST = "qqq.vvv.service";
    public static final Integer BROKER_PORT = 4080;
    public static final String BROKER_END_POINT = "/endpoint";
    public static final String ANOMALY_TIMESTAMP = "1509845454";
    public static final String GROUP_BY_FILTERS = "filter1, filter2";
    public static final String METRIC_NAME = "metric1";


    /**
     * Returns new jobMetadata object for testing.
     * @return jobMetadata Object
     */
    public static JobMetadata getNewJob() {
        JobMetadata jobMetadata = new JobMetadata();
        jobMetadata.setJobId(null);
        jobMetadata.setOwner(JOB_OWNER);
        jobMetadata.setOwnerEmail(JOB_OWNER_EMAIL);
        jobMetadata.setUserQuery(USER_QUERY);
        jobMetadata.setQuery(QUERY);
        jobMetadata.setTestName(TEST_NAME);
        jobMetadata.setTestDescription(TEST_DESCRIPTION);
        jobMetadata.setUrl(URL);
        jobMetadata.setJobStatus(JOB_STATUS);
        jobMetadata.setFrequency(JOB_FREQUENCY);
        jobMetadata.setGranularity(JOB_GRANULARITY);
        jobMetadata.setSigmaThreshold(SIGMA_VALUE);
        jobMetadata.setEffectiveRunTime(JOB_NEXT_RUN_TIME);
        jobMetadata.setClusterId(CLUSTER_ID);
        return jobMetadata;
    }

    /**
     * Returns new anomalyReport object for testing.
     * @return anomalyReport Object
     */
    public static AnomalyReport getNewReport() {
        AnomalyReport anomalyReport = new AnomalyReport();
        anomalyReport.setAnomalyTimestamps(ANOMALY_TIMESTAMP);
        anomalyReport.setGroupByFilters(GROUP_BY_FILTERS);
        anomalyReport.setMetricName(METRIC_NAME);
        anomalyReport.setQueryURL(URL);
        anomalyReport.setReportQueryEndTime(REPORT_NOMINAL_TIME);
        anomalyReport.setUniqueId(UNIQUE_ID);
        anomalyReport.setJobId(JOB_ID);
        return anomalyReport;
    }

    /**
     * Returns new druidCluster object for testing.
     * @return druidCluster Object
     */
    public static DruidCluster getNewDruidCluster() {
        DruidCluster druidCluster = new DruidCluster();
        druidCluster.setClusterId(null);
        druidCluster.setClusterName(CLUSTER_NAME);
        druidCluster.setBrokerEndpoint(BROKER_END_POINT);
        druidCluster.setBrokerHost(BROKER_HOST);
        druidCluster.setBrokerPort(BROKER_PORT);
        druidCluster.setClusterDescription(CLUSTER_DESCRIPTION);
        druidCluster.setHoursOfLag(CLUSTER_SLA);
        return druidCluster;
    }
}
