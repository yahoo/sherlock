/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class UserQueryTest {

    public static UserQuery getUserQuery() {
        UserQuery query = new UserQuery();
        query.setQuery("query");
        query.setTestName("testName");
        query.setTestDescription("testDescription");
        query.setQueryUrl("queryUrl");
        query.setOwner("owner");
        query.setOwnerEmail("ownerEmail");
        query.setGranularity("granularity");
        query.setFrequency("frequency");
        query.setSigmaThreshold(3.0);
        query.setDruidUrl("druidUrl");
        query.setClusterId(1);
        query.setHoursOfLag(24);
        return query;
    }

    /**
     * Helper method to set up a Prophet-related user query.
     */
    public static UserQuery getUserProphetQuery() {
        UserQuery query = new UserQuery();
        query.setQuery("query");
        query.setTestName("testName");
        query.setTestDescription("testDescription");
        query.setQueryUrl("queryUrl");
        query.setOwner("owner");
        query.setOwnerEmail("ownerEmail");
        query.setGranularity("granularity");
        query.setFrequency("frequency");
        query.setSigmaThreshold(4.5);
        query.setDruidUrl("druidUrl");
        query.setClusterId(1);
        query.setHoursOfLag(24);
        query.setTsFramework("Prophet");
        query.setTsModels("Prophet");
        query.setAdModels("KSigmaModel");
        query.setGrowthModel("flat");
        query.setYearlySeasonality("auto");
        query.setWeeklySeasonality("true");
        query.setDailySeasonality("false");
        return query;
    }

    /**
     * Helper method to set up an Egads-related user query.
     */
    public static UserQuery getUserEgadsQuery() {
        UserQuery query = new UserQuery();
        query.setQuery("query");
        query.setTestName("testName");
        query.setTestDescription("testDescription");
        query.setQueryUrl("queryUrl");
        query.setOwner("owner");
        query.setOwnerEmail("ownerEmail");
        query.setGranularity("granularity");
        query.setFrequency("frequency");
        query.setSigmaThreshold(1.0);
        query.setDruidUrl("druidUrl");
        query.setClusterId(1);
        query.setHoursOfLag(24);
        query.setTsFramework("Egads");
        query.setTsModels("RegressionModel");
        query.setAdModels("NaiveModel");
        return query;
    }

    @Test
    public void testUserQueryFields() {
        UserQuery query = getUserQuery();
        assertEquals(query.getQuery(), "query");
        assertEquals(query.getTestName(), "testName");
        assertEquals(query.getTestDescription(), "testDescription");
        assertEquals(query.getQueryUrl(), "queryUrl");
        assertEquals(query.getOwner(), "owner");
        assertEquals(query.getOwnerEmail(), "ownerEmail");
        assertEquals(query.getGranularity(), "granularity");
        assertEquals(query.getFrequency(), "frequency");
        assertEquals(query.getSigmaThreshold(), Double.valueOf(3.0));
        assertEquals(query.getDruidUrl(), "druidUrl");
        assertEquals(query.getClusterId(), (Integer) 1);
        assertEquals(query.getHoursOfLag(), (Integer) 24);
    }

    /**
     * Tests the query fields matches when user does a Prophet-related query.
     */
    @Test
    public void testUserProphetQueryFields() {
        UserQuery query = getUserProphetQuery();
        assertEquals(query.getQuery(), "query");
        assertEquals(query.getTestName(), "testName");
        assertEquals(query.getTestDescription(), "testDescription");
        assertEquals(query.getQueryUrl(), "queryUrl");
        assertEquals(query.getOwner(), "owner");
        assertEquals(query.getOwnerEmail(), "ownerEmail");
        assertEquals(query.getGranularity(), "granularity");
        assertEquals(query.getFrequency(), "frequency");
        assertEquals(query.getSigmaThreshold(), Double.valueOf(4.5));
        assertEquals(query.getDruidUrl(), "druidUrl");
        assertEquals(query.getClusterId(), (Integer) 1);
        assertEquals(query.getHoursOfLag(), (Integer) 24);
        assertEquals(query.getTsFramework(), "Prophet");
        assertEquals(query.getTsModels(), "Prophet");
        assertEquals(query.getAdModels(), "KSigmaModel");
        assertEquals(query.getGrowthModel(), "flat");
        assertEquals(query.getYearlySeasonality(), "auto");
        assertEquals(query.getWeeklySeasonality(), "true");
        assertEquals(query.getDailySeasonality(), "false");
    }

    /**
     * Tests the query fields matches when user does a Egads-related query.
     */
    @Test
    public void testUserEgadsQueryFields() {
        UserQuery query = getUserEgadsQuery();
        assertEquals(query.getQuery(), "query");
        assertEquals(query.getTestName(), "testName");
        assertEquals(query.getTestDescription(), "testDescription");
        assertEquals(query.getQueryUrl(), "queryUrl");
        assertEquals(query.getOwner(), "owner");
        assertEquals(query.getOwnerEmail(), "ownerEmail");
        assertEquals(query.getGranularity(), "granularity");
        assertEquals(query.getFrequency(), "frequency");
        assertEquals(query.getSigmaThreshold(), Double.valueOf(1.0));
        assertEquals(query.getDruidUrl(), "druidUrl");
        assertEquals(query.getClusterId(), (Integer) 1);
        assertEquals(query.getHoursOfLag(), (Integer) 24);
        assertEquals(query.getTsFramework(), "Egads");
        assertEquals(query.getTsModels(), "RegressionModel");
        assertEquals(query.getAdModels(), "NaiveModel");
    }
}
