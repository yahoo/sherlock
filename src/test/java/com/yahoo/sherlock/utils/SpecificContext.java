/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

import java.util.Properties;

public class SpecificContext implements SslContextProvider {

    private final Properties properties;

    public SpecificContext(Properties properties) {
        this.properties = properties;
    }

    @Override
    public SSLConnectionSocketFactory createConnectionSocketFactory() {
        return null;
    }
}
