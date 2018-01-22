/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.google.gson.JsonArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class JsonDataPointTest {

    @Test
    public void testJsonDataPointFields() {
        JsonDataPoint p = new JsonDataPoint();
        JsonArray array = new JsonArray();
        array.add(5);
        p.setResult(array);
        assertEquals(p.getResult(), array);
        JsonArray array1 = new JsonArray();
        array.add(10);
        p.setEvent(array1);
        assertEquals(p.getEvent(), array1);
        String timestamp = "12345";
        p.setTimestamp(timestamp);
        assertEquals(p.getTimestamp(), timestamp);
    }

}
