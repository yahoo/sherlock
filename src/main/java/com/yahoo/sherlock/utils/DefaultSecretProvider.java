/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Default secret provider class.
 */
public class DefaultSecretProvider implements SecretProvider {

    /** Map to hold secrets. **/
    private static final Map<String, String> SECRETS_MAP;

    static {
        SECRETS_MAP = new HashMap<>();
        SECRETS_MAP.put(Constants.TRUSTSTORE_PASS, CLISettings.TRUSTSTORE_PASSWORD);
        SECRETS_MAP.put(Constants.KEYSTORE_PASS, CLISettings.KEYSTORE_PASSWORD);
        SECRETS_MAP.put(Constants.REDIS_PASS, CLISettings.REDIS_PASSWORD);
    }

    /** Constructor. **/
    public DefaultSecretProvider() {

    }

    @Override
    public String getKey(String keyName) {
        return SECRETS_MAP.get(keyName);
    }
}
