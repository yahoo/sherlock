/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.query;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.yahoo.sherlock.enums.Granularity;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashSet;

/**
 * Tests the Query class.
 */
public class QueryTest {

    /**
     * Tests constructor.
     */
    @Test
    public void testQuery() {
        Query query;

        try {
            //null arguments
            query = new Query(null, 213, 1234, Granularity.HOUR, 1);
            Assert.assertNull(query.getQueryJsonObject());
            Assert.assertNull(query.getGroupByDimensions());
            Assert.assertEquals(0, query.getMetricNames().size());
            Assert.assertNull(query.getDatasource());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            Gson gson = new Gson();
            String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_2.json")));
            JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
            query = new Query(queryJsonObject, 123, 1234, Granularity.HOUR, 1);
            LinkedHashSet<String> dimExpected = new LinkedHashSet<String>() {
                {
                    add("dim1");
                }
            };
            JsonElement expectedElement = gson.toJsonTree("s1");
            Assert.assertEquals(dimExpected, query.getGroupByDimensions());
            Assert.assertEquals(Collections.singletonList("m3"), query.getMetricNames());
            Assert.assertEquals(expectedElement, query.getDatasource());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            Gson gson = new Gson();
            String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_4.json")));
            JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
            query = new Query(queryJsonObject, 123, 1234, Granularity.HOUR, 1);
            LinkedHashSet<String> dimExpected = new LinkedHashSet<String>() {
                {
                    add("dim1");
                }
            };
            JsonElement expectedElement = gson.toJsonTree(new String[]{"s1" , "s2"});
            Assert.assertEquals(dimExpected, query.getGroupByDimensions());
            Assert.assertEquals(Collections.singletonList("m3"), query.getMetricNames());
            Assert.assertTrue(query.getDatasource().isJsonArray());
            Assert.assertEquals(expectedElement, query.getDatasource());
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
