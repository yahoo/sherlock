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
import com.yahoo.sherlock.exception.SherlockException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Test class for JsonTimeSeries.
 */
public class JsonTimeSeriesTest {

    /** JsonTimeSeries class object to test. */
    private JsonTimeSeries jsonTimeSeriesWithResultJsonArray, jsonTimeSeriesWithEvent, jsonTimeSeriesWithResultJsonObject;
    /** gson to convert from string to json. */
    private Gson gson = new Gson();

    JsonArray jsonArray;
    Query query;
    /**
     * Setup method to initialize json Time-Series.
     * @throws Exception exception
     */
    @BeforeMethod
    public void setUp() throws Exception {
        // setting druid response with 'result' as a json array
        String druidResponse = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_valid_response_2.json")));
        jsonArray = gson.fromJson(druidResponse, JsonArray.class);
        // setting query object with json query object
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_2.json")));
        JsonObject queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        query = new Query(queryJsonObject, 1234, Granularity.HOUR);
        jsonTimeSeriesWithResultJsonArray = new JsonTimeSeries(jsonArray, query);

        // setting druid response with 'event' as a json array
        druidResponse = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_valid_response_1.json")));
        jsonArray = gson.fromJson(druidResponse, JsonArray.class);
        // setting query object with json query object
        queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_1.json")));
        queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        query = new Query(queryJsonObject, 1234, Granularity.HOUR);
        jsonTimeSeriesWithEvent = new JsonTimeSeries(jsonArray, query);

        // setting druid response with 'event' as a json array
        druidResponse = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_valid_response_3.json")));
        jsonArray = gson.fromJson(druidResponse, JsonArray.class);
        // setting query object with json query object
        queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_3.json")));
        queryJsonObject = gson.fromJson(queryString, JsonObject.class);
        query = new Query(queryJsonObject, 1234, Granularity.HOUR);
        jsonTimeSeriesWithResultJsonObject = new JsonTimeSeries(jsonArray, query);
    }

    /**
     * Test getGroupByDimensionValues() method for valid input.
     * @throws Exception exception
     */
    @Test
    public void testGetGroupByDimensionValues() throws Exception {
        String expected = "dim1 = \'xyz\'";
        String inputBlob = "{\n"
                           + "    \"dim1\" : \"xyz\",\n"
                           + "    \"m1\" : 111,\n"
                           + "    \"m2\" : 222,\n"
                           + "    \"m3\" : 333\n"
                           + "  }";
        JsonElement jsonElement = gson.fromJson(inputBlob , JsonElement.class);
        Method method = JsonTimeSeries.class.getDeclaredMethod("getGroupByDimensionValues", JsonElement.class);
        method.setAccessible(true);
        Assert.assertEquals(expected, method.invoke(jsonTimeSeriesWithResultJsonArray, jsonElement));
    }

    /**
     * Test for null pointer exception in getGroupByDimensionValues() method for null input.
     * @throws Throwable thrown exception
     */
    @Test(expectedExceptions = NullPointerException.class)
    public void testGetGroupByDimensionValuesForNull() throws Throwable {
        JsonElement jsonElement = gson.fromJson((String) null, JsonElement.class);
        Method method = JsonTimeSeries.class.getDeclaredMethod("getGroupByDimensionValues", JsonElement.class);
        method.setAccessible(true);
        try {
            method.invoke(jsonTimeSeriesWithResultJsonArray, jsonElement);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Test for null pointer exception in getGroupByDimensionValues() method for no match in dimension values.
     * @throws Throwable thrown exception
     */
    @Test(expectedExceptions = NullPointerException.class)
    public void testGetGroupByDimensionValuesNoMatch() throws Throwable {
        String inputBlob = "{\n"
                           + "    \"yyyyyy\" : \"xxxxx\",\n"
                           + "    \"metric1\" : 111,\n"
                           + "    \"metric2\" : 111,\n"
                           + "    \"metric3\" : 111\n"
                           + "  }";
        JsonElement jsonElement = gson.fromJson(inputBlob , JsonElement.class);
        Method method = JsonTimeSeries.class.getDeclaredMethod("getGroupByDimensionValues", JsonElement.class);
        method.setAccessible(true);
        try {
            method.invoke(jsonTimeSeriesWithResultJsonArray, jsonElement);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Test parseTimeStampValidInput() for valid input timestamp.
     * @throws Throwable thrown exception
     */
    @Test
    public void testParseTimeStampValidInput() throws Throwable {
        String timestamp = "2017-10-12T00:00:00.000Z";
        Long expected = 1507766400L;
        Method method = JsonTimeSeries.class.getDeclaredMethod("parseTimeStamp", String.class);
        method.setAccessible(true);
        Assert.assertEquals(method.invoke(jsonTimeSeriesWithResultJsonArray, timestamp), expected);
    }

    @Test(expectedExceptions = SherlockException.class)
    public void testParseTimeStampInvalidFormat() throws Throwable {
        String timestamp = "2017-10-12 00:00:00";
        Method method = JsonTimeSeries.class.getDeclaredMethod("parseTimeStamp", String.class);
        method.setAccessible(true);
        try {
            method.invoke(jsonTimeSeriesWithResultJsonArray, timestamp);
            Assert.fail();
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test parseTimeStampValidInput() for null input.
     * @throws Exception exception
     */
    @Test
    public void testParseTimeStampWithNullInput() throws Exception {
        Method method = JsonTimeSeries.class.getDeclaredMethod("parseTimeStamp", String.class);
        method.setAccessible(true);
        try {
            method.invoke(jsonTimeSeriesWithResultJsonArray, (String) null);
            Assert.fail();
        } catch (InvocationTargetException e) {
            Assert.assertEquals(e.getCause().getMessage(), "Null Timestamp in Druid response");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test processJsonDataPointValidData() for valid input.
     * @throws Exception exception
     */
    @Test
    public void testProcessJsonDataPointValidData() throws Exception {
        // test for 'result' json array type output
        for (int i = 0; i < jsonTimeSeriesWithResultJsonArray.getJsonDataSequence().size() ; i++) {
            jsonTimeSeriesWithResultJsonArray
                .processJsonDataPoint(jsonTimeSeriesWithResultJsonArray.getJsonDataSequence().get(i));
        }
        // test for 'event' type output
        for (int i = 0; i < jsonTimeSeriesWithEvent.getJsonDataSequence().size() ; i++) {
            jsonTimeSeriesWithEvent.processJsonDataPoint(jsonTimeSeriesWithEvent.getJsonDataSequence().get(i));
        }
        // test for 'result' json object type output
        for (int i = 0; i < jsonTimeSeriesWithResultJsonObject.getJsonDataSequence().size() ; i++) {
            jsonTimeSeriesWithResultJsonObject.processJsonDataPoint(jsonTimeSeriesWithResultJsonObject.getJsonDataSequence().get(i));
        }
    }

    /**
     * Test processJsonDataPointValidData() for null input.
     * @throws Exception exception
     */
    @Test
    public void testProcessJsonDataPointNullData() throws Exception {
        try {
            jsonTimeSeriesWithResultJsonArray.processJsonDataPoint(null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Null datapoint in Druid response");
        }
    }

}
