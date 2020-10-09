/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.settings.Constants;

import java.util.HashMap;
import java.util.Map;

public class DummySecretProvider implements SecretProvider {
    /** Map to hold secrets. **/
    private static final Map<String, String> SECRETS_MAP;

    static {
        SECRETS_MAP = new HashMap<>();
        SECRETS_MAP.put(Constants.TRUSTSTORE_PASS, "trust_pwd");
        SECRETS_MAP.put(Constants.KEYSTORE_PASS, "key_pwd");
        SECRETS_MAP.put(Constants.REDIS_PASS, "redis_pwd");
    }

    @Override
    public String getKey(String keyName) {
        return SECRETS_MAP.get(keyName);
    }
}
