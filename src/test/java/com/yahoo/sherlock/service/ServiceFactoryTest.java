/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for service factory.
 */
public class ServiceFactoryTest {

    private ServiceFactory serviceFactory;

    @BeforeMethod
    public void setUp() throws Exception {
        serviceFactory = new ServiceFactory();
    }

    @Test
    public void testNewDetectorServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newDetectorServiceInstance() != null);
    }

    @Test
    public void testNewDruidQueryServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newDruidQueryServiceInstance() != null);
    }

    @Test
    public void testNewSchedulerServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newSchedulerServiceInstance() != null);
    }
    
    @Test
    public void testNewEgadsServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newEgadsServiceInstance() != null);
    }

    @Test
    public void testNewEmailServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newEmailServiceInstance() != null);
    }

    @Test
    public void testNewHttpServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newHttpServiceInstance() != null);
    }

    @Test
    public void testNewTimeSeriesParserServiceInstance() throws Exception {
        Assert.assertTrue(serviceFactory.newTimeSeriesParserServiceInstance() != null);
    }
}
