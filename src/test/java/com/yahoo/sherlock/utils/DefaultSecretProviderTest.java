/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.settings.Constants;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class DefaultSecretProviderTest {

    private static final DefaultSecretProvider DSP = new DefaultSecretProvider();

    @Test
    public void testGetKey() {
        Map<String, String> secretsMap = new HashMap<>();
        secretsMap.put(Constants.REDIS_PASS, "redis_password123");
        secretsMap.put(Constants.TRUSTSTORE_PASS, "cert_password456");
        secretsMap.put(Constants.KEYSTORE_PASS, "key_password789");
        TestUtilities.injectStaticFinal(DSP, DefaultSecretProvider.class, "SECRETS_MAP", secretsMap);
        assertEquals(DSP.getKey(Constants.REDIS_PASS), "redis_password123");
        assertEquals(DSP.getKey(Constants.TRUSTSTORE_PASS), "cert_password456");
        assertEquals(DSP.getKey(Constants.KEYSTORE_PASS), "key_password789");
    }
}
