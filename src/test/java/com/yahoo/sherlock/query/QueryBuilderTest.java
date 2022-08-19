/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.query;

import com.google.gson.JsonObject;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.settings.QueryConstants;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.utils.TimeUtils;

import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class QueryBuilderTest {

    public static Object getValue(Field field, Object o) {
        field.setAccessible(true);
        try {
            return field.get(o);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.toString());
        }
    }

    private static ZonedDateTime utcNow() {
        return ZonedDateTime.now(ZoneOffset.UTC);
    }

    /**
     * Recursively search a field in the class hierarchy.
     * @param classVar the class variable
     * @param name the field name
     * @throws NoSuchFieldException if a field is not found
     * @throws SecurityException if there is a security violation
     */
    public static Field getFieldHelper(Class<?> classVar, String name) throws NoSuchFieldException, SecurityException {
        Field field = null;
        while (classVar != null && field == null) {
            try {
                field = classVar.getDeclaredField(name);

            } catch (Exception e) {
                classVar = classVar.getSuperclass();
                if (classVar == null) {
                    throw e;
                }
            }
        }
        return field;
    }

    public static Object getValue(String name, Object o) {
        try {
            Field f = getFieldHelper(o.getClass(), name);
            return getValue(f, o);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e.toString());
        }
    }

    @Test
    public void testQueryBuilderSetters() {
        ZonedDateTime d = utcNow();
        QueryBuilder qb = QueryBuilder.start().endAt(d);
        assertEquals(d, getValue("endTime", qb));
        String dateString = "1990-01-01T00:00";
        qb.endAt(dateString);
        assertNotEquals(d, getValue("endTime", qb));
        qb.startAt(d);
        assertEquals(d, getValue("startTime", qb));
        qb.startAt(dateString);
        assertNotEquals(d, getValue("startTime", qb));
        qb.granularity("hour");
        assertEquals(Granularity.HOUR, getValue("granularity", qb));
        qb.intervals(10);
        assertEquals(10, getValue("intervals", qb));
        qb.intervals("123");
        assertEquals(123, getValue("intervals", qb));
        qb.intervals("-123");
        assertEquals(123, getValue("intervals", qb));
        qb.intervals("abc");
        assertEquals(123, getValue("intervals", qb));
        assertEquals(Granularity.HOUR, qb.getGranularity());
        assertEquals(123, qb.getIntervals());
        try {
            qb.preBuild();
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Empty query string provided");
            return;
        }
        fail();
    }

    @Test
    public void testQueryBuilderBuild() {
        QueryBuilder qb = QueryBuilder.start().queryString("{}");
        try {
            qb.preBuild();
        } catch (SherlockException e) {
            fail(e.toString());
        }
    }

    @Test
    public void testValidateJsonObjectException() {
        JsonObject o = new JsonObject();
        try {
            QueryBuilder.validateJsonObject(o);
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Druid query is missing parameters");
            return;
        }
        fail();
    }

    @Test
    public void testValidateJsonObject() throws SherlockException {
        JsonObject o = new JsonObject();
        o.addProperty(QueryConstants.AGGREGATIONS, "");
        o.addProperty(QueryConstants.INTERVALS, "");
        o.addProperty(QueryConstants.GRANULARITY, "");
        QueryBuilder.validateJsonObject(o);
    }

    @Test
    public void testAsDruidDateNull() {
        assertNull(QueryBuilder.asDruidDate(null));
    }

    @Test
    public void testBuild() throws IOException, SherlockException {
        String expectedStart = "2018-04-05T00:00";
        String expectedEnd = "2018-04-07T00:00";
        String queryString = new String(Files.readAllBytes(Paths.get("src/test/resources/druid_query_2.json")));
        Query query = QueryBuilder.start()
            .endAt(TimeUtils.parseDateTime(expectedEnd))
            .granularity(Granularity.DAY)
            .granularityRange(1)
            .queryString(queryString)
            .intervals(2)
            .build();
        String expectedInterval = QueryBuilder.getInterval(TimeUtils.parseDateTime(expectedStart), TimeUtils.parseDateTime(expectedEnd));
        String expectedOrigin = QueryBuilder.asDruidOrigin(TimeUtils.parseDateTime(expectedStart));
        JsonObject granularityJsonObject = query.getQueryJsonObject().getAsJsonObject(QueryConstants.GRANULARITY);
        assertEquals(query.getQueryJsonObject().getAsJsonPrimitive(QueryConstants.INTERVALS).getAsString(), expectedInterval);
        assertTrue(granularityJsonObject.has(QueryConstants.PERIOD));
        assertTrue(granularityJsonObject.getAsJsonPrimitive(QueryConstants.PERIOD).getAsString().equals(Granularity.DAY.getValue()));
        assertTrue(granularityJsonObject.has(QueryConstants.TYPE));
        assertTrue(granularityJsonObject.getAsJsonPrimitive(QueryConstants.TYPE).getAsString().equals(QueryConstants.PERIOD));
        assertTrue(granularityJsonObject.has(QueryConstants.TIMEZONE));
        assertTrue(granularityJsonObject.getAsJsonPrimitive(QueryConstants.TIMEZONE).getAsString().equals(QueryConstants.UTC));
        assertTrue(granularityJsonObject.has(QueryConstants.ORIGIN));
        assertTrue(granularityJsonObject.getAsJsonPrimitive(QueryConstants.ORIGIN).getAsString().equals(expectedOrigin));
    }

}
