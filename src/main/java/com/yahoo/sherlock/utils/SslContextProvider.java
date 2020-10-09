/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

/**
 * Interface to build SSLContext.
 */
public interface SslContextProvider {
    /**
     * creator method for SSLConnectionSocketFactory.
     * @return SSLConnectionSocketFactory
     **/
    SSLConnectionSocketFactory createConnectionSocketFactory();
}
