/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.sherlock.exception.SherlockException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class DruidClusterTest {

    @Test
    public void testParameterConstructor() {
        DruidCluster c = new DruidCluster(
                1,
                "clusterName",
                "clusterDescription",
                "brokerHost",
                123,
                "brokerEndpoint",
                1234
        );
        assertEquals(c.getClusterId(), (Integer) 1);
        assertEquals(c.getClusterName(), "clusterName");
        assertEquals(c.getClusterDescription(), "clusterDescription");
        assertEquals(c.getBrokerEndpoint(), "brokerEndpoint");
        assertEquals(c.getBrokerHost(), "brokerHost");
        assertEquals(c.getBrokerPort(), (Integer) 123);
        assertEquals(c.getHoursOfLag(), (Integer) 1234);
    }

    @Test
    public void testGetStatusReturnsErrorOnFailedConnection() {
        DruidCluster c = new DruidCluster();
        c.setBrokerHost("localdfffhost");
        c.setBrokerPort(1);
        c.setBrokerEndpoint("asefg");
        assertEquals("ERROR", c.getStatus());
    }

    @Test
    public void testGetBaseUrl() {
        DruidCluster c = new DruidCluster();
        c.setBrokerHost("localhost");
        c.setBrokerPort(1234);
        assertEquals(c.getBaseUrl(), "http://localhost:1234/");
    }

    @Test
    public void testGetBrokerUrl() {
        DruidCluster c = new DruidCluster();
        c.setBrokerHost("localhost");
        c.setBrokerPort(1234);
        c.setBrokerEndpoint("druid/v2");
        assertEquals(c.getBrokerUrl(), "http://localhost:1234/druid/v2/");
    }

    @Test
    public void testValidateInvalidName() {
        DruidCluster c = new DruidCluster();
        try {
            c.validate();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Cluster name cannot be empty");
            return;
        }
        fail("Expected exception");
    }

    @Test
    public void testValidateInvalidHost() {
        DruidCluster c = new DruidCluster();
        c.setClusterName("hello");
        try {
            c.validate();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Broker host cannot be empty");
            return;
        }
        fail("Expected exception");
    }

    @Test
    public void testValidateInvalidPort() {
        DruidCluster c = new DruidCluster();
        c.setClusterName("hello");
        c.setBrokerHost("host");
        try {
            c.validate();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Broker port cannot be empty");
            return;
        }
        fail("Expected exception");
    }

    @Test
    public void testValidateInvalidEndpoint() {
        DruidCluster c = new DruidCluster();
        c.setClusterName("hello");
        c.setBrokerHost("host");
        c.setBrokerPort(1234);
        try {
            c.validate();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Broker endpoint cannot be empty");
            return;
        }
        fail("Expected exception");
    }

    @Test
    public void testValidateNonNumericalPort() {
        DruidCluster c = new DruidCluster();
        c.setClusterName("hello");
        c.setBrokerHost("host");
        c.setBrokerPort(-12345);
        c.setBrokerEndpoint("druid/v2");
        try {
            c.validate();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Broker port must be a non-negative number");
            return;
        }
        fail("Expected exception");
    }

    @Test
    public void testValidateBrokerHostInvalidCharacters() {
        DruidCluster c = new DruidCluster();
        c.setClusterName("helo");
        c.setBrokerHost("asdf/asdf");
        c.setBrokerPort(1234);
        c.setBrokerEndpoint("druid/v2");
        try {
            c.validate();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Broker host should not contain any '/' characters");
            return;
        }
        fail("Expected exception");
    }

    @Test
    public void testValidateDescription() {
        DruidCluster c = new DruidCluster();
        c.setClusterName("hello");
        c.setBrokerHost("localhost");
        c.setBrokerPort(1234);
        c.setBrokerEndpoint("/druid/v2/");
        try {
            c.validate();
            assertEquals(c.getBrokerEndpoint(), "druid/v2");
            assertEquals(c.getClusterDescription(), "");
        } catch (SherlockException e) {
            fail(e.toString());
        }
    }

}
