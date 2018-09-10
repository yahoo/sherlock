/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.settings;

import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CLISettingsTest {

    public static Field getField(String name, Class<?> cls) {
        try {
            Field f = cls.getDeclaredField(name);
            return f;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void setField(Field field, Object value) {
        try {
            field.setAccessible(true);
            field.set(null, value);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Object fieldVal(Field field) {
        try {
            field.setAccessible(true);
            return field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void makeConfigFile(String[] keys, String[] values) {
        File f = new File("TestConfigFile.ini");
        if (f.exists()) {
            if (!f.delete()) {
                throw new RuntimeException("Failed to create test config file");
            }
        }
        try {
            if (!f.createNewFile()) {
                throw new Exception("Failed to create test config file");
            }
            BufferedWriter w = new BufferedWriter(new FileWriter(f));
            for (int i = 0; i < keys.length; i++) {
                w.write(String.format("%s %s%n", keys[i], values[i]));
            }
            w.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void deleteConfigFile() {
        File f = new File("TestConfigFile.ini");
        if (f.exists()) {
            if (!f.delete()) {
                throw new RuntimeException("Failed to delete test file");
            }
        }
    }

    @Test
    public void testLoadFromConfigIgnoresConfigFileWhenNull() throws IOException {
        Field configFile = getField("CONFIG_FILE", CLISettings.class);
        Object configFileOrg = fieldVal(configFile);
        Field backend = getField("VERSION", CLISettings.class);
        Object backendOrg = fieldVal(backend);

        setField(configFile, null);
        setField(backend, "testDB");
        makeConfigFile(new String[] {"version"}, new String[] {"Redis"});
        CLISettings settings = new CLISettings();
        settings.loadFromConfig();
        Object backendVal = fieldVal(backend);
        assertEquals(backendVal, "testDB");
        deleteConfigFile();

        setField(backend, backendOrg);
        setField(configFile, configFileOrg);
    }

    @Test
    public void testLoadFromConfigLoadsConfigFiles() throws IOException {
        Field configFile = getField("CONFIG_FILE", CLISettings.class);
        Object configFileOrg = fieldVal(configFile);
        Field backend = getField("VERSION", CLISettings.class);
        Object backendOrg = fieldVal(backend);

        setField(configFile, "TestConfigFile.ini");
        setField(backend, "testDB");
        makeConfigFile(new String[] {"version"}, new String[] {"Redis"});
        CLISettings settings = new CLISettings();
        settings.loadFromConfig();
        Object backendVal = fieldVal(backend);
        assertEquals(backendVal, "Redis");
        deleteConfigFile();

        setField(backend, backendOrg);
        setField(configFile, configFileOrg);
    }

    @Test
    public void testLoadFromConfigFileThrowsOnInvalidValue() throws IllegalAccessException {
        Field ignored = getField("PRINT_IGNORED", CLISettings.class);
        ignored.setAccessible(true);
        Object original = ignored.get(null);
        String[] newIgnored = {"log", "REDIS_PASSWORD"};
        ignored.set(null, newIgnored);
        ignored.set(null, original);

        Field configFile = getField("CONFIG_FILE", CLISettings.class);
        Object configFileOrg = fieldVal(configFile);

        setField(configFile, "TestConfigFile.ini");
        makeConfigFile(new String[] {"port"}, new String[] {"Redis"});
        CLISettings settings = new CLISettings();
        try {
            settings.loadFromConfig();
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Failed to set"));
        }
        deleteConfigFile();

        setField(configFile, configFileOrg);
    }

    @Test
    public void testPrintDoesNotThrowError() throws IllegalAccessException {
        Field ignored = getField("PRINT_IGNORED", CLISettings.class);
        ignored.setAccessible(true);
        Object original = ignored.get(null);
        String[] newIgnored = {"log", "REDIS_PASSWORD"};
        ignored.set(null, newIgnored);
        CLISettings settings = new CLISettings();
        settings.print();
        ignored.set(null, original);
    }

}
