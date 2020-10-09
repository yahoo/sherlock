/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

public class SSslTestHelper {
    private static final String TRUSTSTORE_PATH = "truststorepath";
    private static final String TRUSTSTORE_TYPE = "jks";
    private static final String TRUSTSTORE_PASS = "abc";
    private static final String KEYSTORE_PATH = "keystorepath";
    private static final String KEYSTORE_TYPE = "jks";
    private static final String KEYSTORE_PASS = "xyz";
    private static final String KEY_PATH = "/tmp/key/path";
    private static final String CERT_PATH = "/tmp/cert/path";
    private static final Boolean HOSTNAME_STRICT_VERIFICATION = true;

    public static SSslConfigs getTestSSslConfigs() {
        SSslConfigs sSslConfigs = new SSslConfigs();
        sSslConfigs.setTrustStorePath(TRUSTSTORE_PATH);
        sSslConfigs.setTrustStorePass(TRUSTSTORE_PASS);
        sSslConfigs.setTrustStoreType(TRUSTSTORE_TYPE);
        sSslConfigs.setKeyStorePath(KEYSTORE_PATH);
        sSslConfigs.setKeyStorePass(KEYSTORE_PASS);
        sSslConfigs.setKeyStoreType(KEYSTORE_TYPE);
        sSslConfigs.setCertPath(CERT_PATH);
        sSslConfigs.setKeyPath(KEY_PATH);
        sSslConfigs.setStrict(HOSTNAME_STRICT_VERIFICATION);
        return sSslConfigs;
    }

    public static SSslConfigs.SSslConfigsBuilder getTestSSslConfigsBuilder() {
        SSslConfigs.SSslConfigsBuilder sSslConfigsBuilder = SSslConfigs.SSslConfigsBuilder.getSSslConfigsBuilder();
        sSslConfigsBuilder.trustStorePath(TRUSTSTORE_PATH);
        sSslConfigsBuilder.trustStorePass(TRUSTSTORE_PASS);
        sSslConfigsBuilder.trustStoreType(TRUSTSTORE_TYPE);
        sSslConfigsBuilder.keyStorePath(KEYSTORE_PATH);
        sSslConfigsBuilder.keyStorePass(KEYSTORE_PASS);
        sSslConfigsBuilder.keyStoreType(KEYSTORE_TYPE);
        sSslConfigsBuilder.certPath(CERT_PATH);
        sSslConfigsBuilder.keyPath(KEY_PATH);
        sSslConfigsBuilder.isStrict(HOSTNAME_STRICT_VERIFICATION);
        return sSslConfigsBuilder;
    }
}
