/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;

public class StoreParamsTest {

    @Test
    public void testFromPropertiesCreatesStoreParams() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        props.setProperty("key3", "value3");
        StoreParams params = StoreParams.fromProperties(props);
        assertEquals(3, params.size());
        String[] expectedKeys = {"key1", "key2", "key3"};
        String[] expectedValues = {"value1", "value2", "value3"};
        String[] actualKeys = params.keySet().toArray(new String[0]);
        String[] actualValues = params.values().toArray(new String[0]);
        assertEqualsNoOrder(expectedKeys, actualKeys);
        assertEqualsNoOrder(expectedValues, actualValues);
    }

    @Test
    public void testBasicGetAndSet() {
        StoreParams params = new StoreParams();
        assertEquals(0, params.size());
        assertNull(params.get("key1"));
        assertNull(params.put("key1", "value1"));
        assertEquals("value1", params.get("key1"));
    }

}
