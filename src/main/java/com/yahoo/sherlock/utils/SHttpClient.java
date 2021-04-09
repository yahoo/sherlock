/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.settings.CLISettings;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * HttpClient provider class.
 */
public class SHttpClient {

    /** Singleton. **/
    private static SHttpClient sHttpClient;

    /** SSslUtils instance. **/
    private SSslUtils sSslUtils = new SSslUtils();

    private SHttpClient() {

    }

    /**
     * Method to retrieve HttpClient instance.
     * @return SHttpClient instance
     */
    public static SHttpClient getSHttpClient() {
        if (sHttpClient == null) {
            sHttpClient = new SHttpClient();
        }
        return sHttpClient;
    }

    /**
     * Method to get new HttpClient.
     *
     * @param timeout the connection timeout
     * @param retries the number of times the client should reattempt connections
     * @param sslAuth enable/disable ssl auth
     * @param principal ssl auth principal string value
     * @return HttpClient object
     */
    public HttpClient newHttpClient(int timeout, int retries, boolean sslAuth, String principal) {
        RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .build();
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
            .setRetryHandler(new DefaultHttpRequestRetryHandler(retries, false))
            .setDefaultRequestConfig(config);
        if (sslAuth) {
            if (principal != null && !principal.isEmpty()) {
                return httpClientBuilder.setSSLSocketFactory(sSslUtils.createConnectionSocketFactoryWithCustomImpl(sSslUtils.buildSSLConfigs(principal), CLISettings.CUSTOM_SSL_CONTEXT_PROVIDER_CLASS)).build();
            }
            DefaultSslContextProvider defaultSSLContext = new DefaultSslContextProvider(sSslUtils.buildSSLConfigs(principal));
            return httpClientBuilder.setSSLSocketFactory(defaultSSLContext.createConnectionSocketFactory()).build();
        }
        return httpClientBuilder.build();
    }

    /**
     * Get a new {@code HttpClient} with a {@link CLISettings#HTTP_CLIENT_TIMEOUT} timeout.
     *
     * @return HttpClient
     */
    public HttpClient newHttpClient() {
        return newHttpClient(CLISettings.HTTP_CLIENT_TIMEOUT, 3, false, "");
    }

    /**
     * Get a new {@code HttpClient} with a ssl Auth param.
     *
     * @param sslAuth enable/disable ssl auth
     * @param principal ssl auth principal string value
     * @return HttpClient
     */
    public HttpClient newHttpClient(boolean sslAuth, String principal) {
        return newHttpClient(CLISettings.HTTP_CLIENT_TIMEOUT, 3, sslAuth, principal);
    }

    /**
     * Get a new post method instance with the default 3 retries.
     *
     * @param url the post URL
     * @return PostMethod
     */
    public HttpPost newHttpPost(String url) {
        return new HttpPost(url);
    }

    /**
     * Get a new get method instance with the default 3 retries.
     *
     * @param url the get URL
     * @return GetMethod
     */
    public HttpGet newHttpGet(String url) {
        return new HttpGet(url);
    }
}
