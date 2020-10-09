/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.utils.DefaultSecretProvider;
import com.yahoo.sherlock.utils.DummySecretProvider;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SecretProviderServiceTest {

    @BeforeTest
    public void beforeTest() {
        CLISettings.CUSTOM_SECRET_PROVIDER_CLASS = DummySecretProvider.class.getCanonicalName();
    }

    @AfterTest
    public void afterTest() {
        CLISettings.CUSTOM_SECRET_PROVIDER_CLASS = DefaultSecretProvider.class.getCanonicalName();
    }

    @Test
    public void testInitSecretProviderAndGetKey() {
        SecretProviderService.initSecretProvider();
        assertEquals(SecretProviderService.getKey(Constants.REDIS_PASS), "redis_pwd");
        assertEquals(SecretProviderService.getKey(Constants.TRUSTSTORE_PASS), "trust_pwd");
        assertEquals(SecretProviderService.getKey(Constants.KEYSTORE_PASS), "key_pwd");
    }
}
