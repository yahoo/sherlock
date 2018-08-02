/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.SherlockException;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Test for druid query service.
 */
public class DruidQueryServiceTest {

    DruidQueryService druidQueryService = new DruidQueryService();

    @Test
    public void testBuildWithValidQuery() throws Exception {
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_1.json")));
        Assert.assertEquals(druidQueryService.build(queryString, Granularity.DAY, null, null, null).getMetricNames(), Collections.singletonList("m1"));
    }

    @Test
    public void testBuildWithInvalidDruidQuery() throws IOException {
        String badQueryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_invalid_query_1.json")));
        try {
            druidQueryService.build(badQueryString, Granularity.DAY, null, null, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Druid query is missing parameters");
        }
    }

    @Test(expectedExceptions = SherlockException.class)
    public void testBuildWithInvalidQueryJson() throws Throwable {
        String badQueryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_invalid_query_4.json")));
        druidQueryService.build(badQueryString, Granularity.DAY, null, null, null);
    }

    @Test
    public void testBuildWithInvalidQuerySyntax() throws Throwable {
        try {
            druidQueryService.build("{}{}{}{{{}}}{}}", Granularity.DAY, null, null, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid query syntax! Check JSON brackets");
        }
    }
}
