/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

/**
 * Default SSLContect to use for mTLS connections.
 */
public class DefaultSslContextProvider implements SslContextProvider {

    /**
     * configs for SSL auth.
     */
    private final SSslConfigs sSslConfigs;

    /**
     * SSL utilities.
     */
    private final SSslUtils sSslUtils = new SSslUtils();

    /** Constructor.
     * @param sSslConfigs ssl configs
     **/
    public DefaultSslContextProvider(SSslConfigs sSslConfigs) {
        this.sSslConfigs = sSslConfigs;
    }

    @Override
    public SSLConnectionSocketFactory createConnectionSocketFactory() {
        return sSslUtils.createConnectionSocketFactoryWithValidation(sSslConfigs);
    }
}
