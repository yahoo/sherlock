/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.settings.Constants;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SSslConfigsTest {

    private SSslConfigs sSslConfigs;
    private SSslConfigs.SSslConfigsBuilder sSslConfigsBuilder;


    @BeforeMethod
    public void setUp() {
        sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        sSslConfigsBuilder = SSslTestHelper.getTestSSslConfigsBuilder();
    }

    @Test
    public void testAsProperties() {
        Properties properties = sSslConfigs.asProperties();
        assertEquals(properties.size(), 9);
        assertEquals(properties.getProperty(Constants.TRUSTSTORE_PATH), sSslConfigs.getTrustStorePath());
        assertEquals(properties.getProperty(Constants.TRUSTSTORE_PASS), sSslConfigs.getTrustStorePass());
        assertEquals(properties.getProperty(Constants.TRUSTSTORE_TYPE), sSslConfigs.getTrustStoreType());
        assertEquals(properties.getProperty(Constants.KEYSTORE_PATH), sSslConfigs.getKeyStorePath());
        assertEquals(properties.getProperty(Constants.KEYSTORE_PASS), sSslConfigs.getKeyStorePass());
        assertEquals(properties.getProperty(Constants.KEYSTORE_TYPE), sSslConfigs.getKeyStoreType());
        assertEquals(properties.getProperty(Constants.KEY_PATH), sSslConfigs.getKeyPath());
        assertEquals(properties.getProperty(Constants.CERT_PATH), sSslConfigs.getCertPath());
        assertEquals(properties.getProperty(Constants.HOSTNAME_STRICT_VERIFICATION), String.valueOf(sSslConfigs.getStrict()));
    }

    @Test
    public void testBuilderConfigs() {
        assertEquals(sSslConfigsBuilder.build(), sSslConfigs);
    }

    @Test
    public void testTestEquals() {
        assertTrue(sSslConfigsBuilder.build().equals(sSslConfigs));
    }
}
