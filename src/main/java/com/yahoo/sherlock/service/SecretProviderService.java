/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.utils.SecretProvider;
import com.yahoo.sherlock.utils.Utils;

import lombok.extern.slf4j.Slf4j;

/**
 * SecretProvider service class.
 */
@Slf4j
public class SecretProviderService {

    /** SecretProvider instance. **/
    private static SecretProvider secretProvider;

    /**
     * Constructor.
     */
    private SecretProviderService() {

    }

    /**
     * Initialize the SecretProvider.
     */
    public static void initSecretProvider() {
        if (secretProvider == null) {
            secretProvider = Utils.createSecretProvider(CLISettings.CUSTOM_SECRET_PROVIDER_CLASS);
        }
    }

    /**
     * Get the secrets for given key.
     * @param keyName name of the key
     * @return secrets as a String
     */
    public static String getKey(String keyName) {
        if (secretProvider == null) {
            log.warn("Secret provider is not initialized! Initializing before retrieving the key...");
            initSecretProvider();
        }
        return secretProvider.getKey(keyName);
    }
}
