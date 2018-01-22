package com.yahoo.sherlock.query;

import com.google.gson.JsonObject;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.settings.QueryConstants;
import com.yahoo.sherlock.enums.Granularity;

import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
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

    public static Object getValue(String name, Object o) {
        try {
            Field f = o.getClass().getDeclaredField(name);
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
    public void testValidateJsonObject() {
        JsonObject o = new JsonObject();
        try {
            QueryBuilder.validateJsonObject(o);
        } catch (SherlockException e) {
            assertEquals(e.getMessage(), "Druid query is missing parameters");
            o.addProperty(QueryConstants.POSTAGGREGATIONS, "");
            o.addProperty(QueryConstants.AGGREGATIONS, "");
            o.addProperty(QueryConstants.INTERVALS, "");
            o.addProperty(QueryConstants.GRANULARITY, "");
            try {
                QueryBuilder.validateJsonObject(o);
            } catch (SherlockException j) {
                assertEquals(j.getMessage(), "Granularity is not a JSON object");
                return;
            }
        }
        fail();
    }

    @Test
    public void testAsDruidDateNull() {
        assertNull(QueryBuilder.asDruidDate(null));
    }

}
