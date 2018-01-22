/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.yahoo.sherlock.exception.StoreException;
import com.yahoo.sherlock.store.Attribute;

import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;

public class HashMapperTest {

    @Data
    public static class TestData {

        @Attribute
        private String attribute1;
        @Attribute private String attribute2;
        @Attribute private String attribute3;

        public TestData() {
        }
        public TestData(String attribute1, String attribute2, String attribute3) {
            this.attribute1 = attribute1;
            this.attribute2 = attribute2;
            this.attribute3 = attribute3;
        }

    }

    @Test
    public void testInitClassSetsClassAndFields() {
        try {
            HashMapper hm = new HashMapper();
            Method init = Mapper.class.getDeclaredMethod("init", Class.class);
            init.setAccessible(true);
            init.invoke(hm, TestData.class);
            Field objClass = Mapper.class.getDeclaredField("objClass");
            Field fields = Mapper.class.getDeclaredField("fields");
            objClass.setAccessible(true);
            fields.setAccessible(true);
            Class<?> objClassVal = (Class<?>) objClass.get(hm);
            Field[] fieldsVal = (Field[]) fields.get(hm);
            assertEquals(objClassVal, TestData.class);
            assertEquals(3, fieldsVal.length);
            String[] expectedFieldNames = {"attribute1", "attribute2", "attribute3"};
            String[] actualFieldNames = new String[3];
            for (int i = 0; i < 3; i++) {
                actualFieldNames[i] = fieldsVal[i].getName();
            }
            assertEqualsNoOrder(expectedFieldNames, actualFieldNames);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            Assert.fail("Could not access method init(): " + e.toString());
        } catch (NoSuchFieldException e) {
            Assert.fail("Could not access field: " + e.toString());
        }
    }

    private void checkInit(HashMapper hm) {
        try {
            Field objClassField = Mapper.class.getDeclaredField("objClass");
            objClassField.setAccessible(true);
            Class<?> setClass = (Class<?>) objClassField.get(hm);
            assertEquals(setClass, TestData.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail("Could not access field: " + e.toString());
        }
    }

    @Test
    public void testHashInitsAndHashesObject() {
        HashMapper hm = new HashMapper();
        TestData td = new TestData("value1", "value2", "value3");
        Map<String, String> result = hm.map(td);
        assertEquals(3, result.size());
        String[] expectedKeys = {"attribute1", "attribute2", "attribute3"};
        String[] expectedValues = {"value1", "value2", "value3"};
        assertEqualsNoOrder(expectedKeys, result.keySet().toArray());
        assertEqualsNoOrder(expectedValues, result.values().toArray());
        checkInit(hm);
    }

    @Test
    public void testHashInitsAndThrowsOnDifferentClass() {
        HashMapper hm = new HashMapper();
        TestData td = new TestData("value1", "value2", "value3");
        hm.map(td);
        try {
            hm.map("different_object_class");
        } catch (StoreException e) {
            return;
        }
        Assert.fail("Expected StoreException to be thrown");
    }

    @Test
    public void testHashReplacesNullsWithEmptyString() {
        HashMapper hm = new HashMapper();
        TestData td = new TestData(null, null, null);
        Map<String, String> result = hm.map(td);
        String[] expectedKeys = {"attribute1", "attribute2", "attribute3"};
        for (String expectedKey : expectedKeys) {
            assertEquals("", result.get(expectedKey));
        }
    }

    @Test
    public void testUnhashInitsAndCreatesObject() {
        HashMapper hm = new HashMapper();
        Map<String, String> hash = new HashMap<String, String>() {
            {
                put("attribute1", "value1");
                put("attribute2", "value2");
                put("attribute3", "value3");
            }
        };
        TestData td = hm.unmap(TestData.class, hash);
        assertEquals(td.attribute1, "value1");
        assertEquals(td.attribute2, "value2");
        assertEquals(td.attribute3, "value3");
        checkInit(hm);
    }

    @Test
    public void testUnhashThrowsOnInvalidSecondClass() {
        HashMapper hm = new HashMapper();
        Map<String, String> hash = new HashMap<String, String>() {
            {
                put("attribute1", "value1");
                put("attribute2", "value2");
                put("attribute3", "value3");
            }
        };
        hm.unmap(TestData.class, hash);
        try {
            hm.unmap(String.class, new HashMap<>());
        } catch (StoreException e) {
            return;
        }
        Assert.fail("Expected StoreException to be thrown");
    }

    @Data
    public static class TestDataNoDefault {

        @Attribute private String attribute;

        private TestDataNoDefault(String attribute) {
            this.attribute = attribute;
        }

    }

    @Test
    public void testUnhashThrowsOnNoDefaultConstructor() {
        HashMapper hm = new HashMapper();
        Map<String, String> hash = new HashMap<>();
        try {
            hm.unmap(TestDataNoDefault.class, hash);
        } catch (StoreException e) {
            assertEquals("Class TestDataNoDefault does not provide a default constructor", e.getMessage());
            return;
        }
        Assert.fail("Expected StoreException to be thrown");
    }

    @Data
    public static class TestDataExceptionCtor {

        @Attribute private String attribute;

        public TestDataExceptionCtor() {
            throw new RuntimeException();
        }

    }

    @Test
    public void testUnhashThrowsOnInaccessibleConstructor() {
        HashMapper hm = new HashMapper();
        Map<String, String> hash = new HashMap<>();
        try {
            hm.unmap(TestDataExceptionCtor.class, hash);
        } catch (StoreException e) {
            assertEquals("Failed to instantiate new object of type TestDataExceptionCtor", e.getMessage());
            return;
        }
        Assert.fail("Expected StoreException to be thrown");
    }

    @Test
    public void testUnhashNullOrEmptyFields() {
        HashMapper hm = new HashMapper();
        Map<String, String> hash = new HashMap<String, String>() {
            {
                put("attribute1", "");
                put("attribute2", null);
                put("attribute3", "value3");
            }
        };
        TestData td = hm.unmap(TestData.class, hash);
        assertNull(td.attribute2);
        assertEquals("", td.attribute1);
        assertEquals("value3", td.attribute3);
    }

}
