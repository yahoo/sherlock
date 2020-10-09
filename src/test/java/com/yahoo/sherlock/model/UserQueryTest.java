/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class UserQueryTest {

    @Test
    public void testUserQueryFields() {
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
    }

}
