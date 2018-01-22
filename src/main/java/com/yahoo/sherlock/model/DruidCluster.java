/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.service.HttpService;
import com.yahoo.sherlock.store.Attribute;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.httpclient.HttpStatus;

import java.io.Serializable;

/**
 * Deserializer class for a Druid cluster. This object is built from a request
 * to add a Druid cluster and then this object is stored in the database with
 * an assigned id.
 */
@Data
@Slf4j
public class DruidCluster implements Serializable {

    /** Serialization id for uniformity across platform. */
    private static final long serialVersionUID = 14L;

    /** Unique ID for the DruidCluster. */
    @Attribute
    private Integer clusterId;

    /** Name of the Druid cluster. */
    @Attribute
    private String clusterName;

    /** An optional description for the Druid cluster. */
    @Attribute
    private String clusterDescription;

    /** Druid cluster broker host name. */
    @Attribute
    private String brokerHost;

    /** Druid cluster broker port. */
    @Attribute
    private Integer brokerPort;

    /** Druid cluster broker endpoint. */
    @Attribute
    private String brokerEndpoint;

    /** Druid cluster data availability lag in hours. */
    @Attribute
    private Integer hoursOfLag;

    /** Empty constructor. */
    public DruidCluster() {
    }

    /**
     * Data initializer constructor.
     * @param clusterId the id of the cluster
     * @param clusterName the name of the cluster
     * @param clusterDescription a description of the cluster
     * @param brokerHost the cluster broker host name
     * @param brokerPort the cluster broker port
     * @param brokerEndpoint the cluster broker endpoint
     * @param hoursOfLag the cluster sla in hours
     */
    public DruidCluster(
        Integer clusterId,
        String clusterName,
        String clusterDescription,
        String brokerHost,
        Integer brokerPort,
        String brokerEndpoint,
        Integer hoursOfLag) {
        this.clusterId = clusterId;
        this.clusterName = clusterName;
        this.clusterDescription = clusterDescription;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.brokerEndpoint = brokerEndpoint;
        this.hoursOfLag = hoursOfLag;
    }

    /**
     * Get a String that represents the status of this cluster.
     * @return "OK" if the cluster can be contacted,
     * "ERROR" if not, or an error code otherwise
     */
    public String getStatus() {
        String clusterStatus;
        try {
            int status = new HttpService().queryDruidClusterStatus(this);
            switch (status) {
                case HttpStatus.SC_OK:
                    clusterStatus = "OK";
                    break;
                default:
                    clusterStatus = String.valueOf(status);
                    break;
            }
        } catch (Exception e) {
            clusterStatus = "ERROR";
        }
        return clusterStatus;
    }

    /**
     * Performs validation on the parameters in this DruidCluster object.
     * @return true if the parameters are valid
     * @throws SherlockException with a message if any parameters are invalid
     */
    public boolean validate() throws SherlockException {
        String errorMsg;
        if (clusterName == null || clusterName.length() == 0) {
            errorMsg = "Cluster name cannot be empty";
        } else if (brokerHost == null || brokerHost.length() == 0) {
            errorMsg = "Broker host cannot be empty";
        } else if (brokerPort == null) {
            errorMsg = "Broker port cannot be empty";
        } else if (brokerEndpoint == null || brokerEndpoint.length() == 0) {
            errorMsg = "Broker endpoint cannot be empty";
        } else if (brokerPort < 0) {
            errorMsg = "Broker port must be a non-negative number";
        } else if (brokerHost.contains("/") || brokerHost.contains(":")) {
            errorMsg = "Broker host should not contain any '/' or ':' characters";
        } else {
            if (clusterDescription == null) {
                clusterDescription = "";
            }
            clusterName = clusterName.trim();
            clusterDescription = clusterDescription.trim();
            brokerHost = brokerHost.trim();
            brokerEndpoint = brokerEndpoint.trim();
            int endpointStart = 0;
            int endpointEnd = brokerEndpoint.length();
            if (brokerEndpoint.charAt(0) == '/') {
                endpointStart++;
            }
            if (brokerEndpoint.charAt(endpointEnd - 1) == '/') {
                endpointEnd--;
            }
            if (endpointStart != 0 || endpointEnd != brokerEndpoint.length()) {
                brokerEndpoint = brokerEndpoint.substring(endpointStart, endpointEnd);
            }
            return true;
        }
        if (hoursOfLag == null || hoursOfLag < 0) {
            hoursOfLag = 0;
        }
        throw new SherlockException(errorMsg);
    }

    /**
     * Update the fields of the cluster with a new Cluster.
     * @param newCluster the new cluster
     */
    public void update(DruidCluster newCluster) {
        setClusterName(newCluster.getClusterName());
        setClusterDescription(newCluster.getClusterDescription());
        setBrokerHost(newCluster.getBrokerHost());
        setBrokerPort(newCluster.getBrokerPort());
        setBrokerEndpoint(newCluster.getBrokerEndpoint());
        setHoursOfLag(newCluster.getHoursOfLag());
    }

    /**
     * Build and get the base broker URL for this cluster.
     * @return base broker URL
     */
    public String getBaseUrl() {
        return String.format("http://%s:%s/", brokerHost, brokerPort);
    }

    /**
     * Build and get the broker URL for this cluster.
     * @return broker URL
     */
    public String getBrokerUrl() {
        return String.format("%s%s/", getBaseUrl(), brokerEndpoint);
    }

}
