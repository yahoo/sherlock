/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

/**
 * Secret provider interface.
 */
public interface SecretProvider {

    /**
     * Method to get secret value for given key.
     * @param keyName name of the secret key
     * @return secret's value as a String
     */
    String getKey(String keyName);
}
