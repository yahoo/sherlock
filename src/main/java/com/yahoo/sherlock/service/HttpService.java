/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.DruidConstants;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Service class for http rest calls.
 */
@Slf4j
public class HttpService {

    /**
     * Method to get new HttpClient.
     *
     * @param timeout the connection timeout
     * @param retries the number of times the client should reattempt connections
     * @return HttpClient object
     */
    protected HttpClient newHttpClient(int timeout, int retries) {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeout)
                .setSocketTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .build();
        return HttpClientBuilder.create()
                .setRetryHandler(new DefaultHttpRequestRetryHandler(retries, false))
                .setDefaultRequestConfig(config)
                .build();
    }

    /**
     * Get a new {@code HttpClient} with a {@link CLISettings#DRUID_QUERY_TIMEOUT} timeout.
     *
     * @return HttpClient
     */
    protected HttpClient newHttpClient() {
        return newHttpClient(CLISettings.DRUID_QUERY_TIMEOUT, 3);
    }

    /**
     * Get a new post method instance with the default 3 retries.
     *
     * @param url the post URL
     * @return PostMethod
     */
    protected HttpPost newHttpPost(String url) {
        return new HttpPost(url);
    }

    /**
     * Get a new get method instance with the default 3 retries.
     *
     * @param url the get URL
     * @return GetMethod
     */
    protected HttpGet newHttpGet(String url) {
        return new HttpGet(url);
    }

    /**
     * /**
     * Service method to call druid.
     *
     * @param cluster    the Druid cluster to issue the query
     * @param druidQuery druid query json object
     * @return druid response as a json array
     * @throws DruidException http request exception while querying druid
     */
    public JsonArray queryDruid(DruidCluster cluster, JsonObject druidQuery) throws DruidException {
        log.info("Calling druid broker.");
        String url = cluster.getBrokerUrl();
        HttpClient client = newHttpClient();
        HttpPost httpPost = newHttpPost(url);
        try {
            HttpEntity httpEntity = new StringEntity(druidQuery.toString(), ContentType.APPLICATION_JSON);
            httpPost.setEntity(httpEntity);
            // Execute query to Druid
            HttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("Post request to broker endpoint failed: {}", response.getStatusLine());
                throw new DruidException("Post request to broker endpoint failed: " + statusCode);
            }
            // Read the response body.
            InputStream inputStream = response.getEntity().getContent();
            Gson gson = new Gson();
            // get the response as json array
            JsonArray jsonArray = gson.fromJson(new InputStreamReader(inputStream), JsonArray.class);
            log.info("Parsed druid response to json array.");
            return jsonArray;
        } catch (Exception e) {
            log.error("Error while sending druid query!", e);
            throw new DruidException(e.getMessage(), e);
        } finally {
            // Release the connection.
            httpPost.releaseConnection();
        }
    }

    /**
     * Issue a `GET` request to the Druid `/datasource/` endpoint
     * to retrieve a list of datasources.
     *
     * @param cluster the Druid cluster to query for datasources
     * @return a JsonArray of datasources on the specified cluster
     * @throws DruidException when an error occurs sending the request or parsing response
     */
    public JsonArray queryDruidDatasources(DruidCluster cluster) throws DruidException {
        log.info("Calling Druid broker for datasource list.");
        String url = cluster.getBrokerUrl() + DruidConstants.DATASOURCES;
        HttpClient client = newHttpClient(2000, 2);
        HttpGet httpGet = newHttpGet(url);
        try {
            HttpResponse response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("Get request to datasources endpoint failed: {}", response.getStatusLine());
                throw new DruidException("Get request to cluster datasources endpoint failed: " + statusCode);
            }
            // Read response body for datasources
            InputStream inputStream = response.getEntity().getContent();
            JsonArray responseArr = new Gson().fromJson(new InputStreamReader(inputStream), JsonArray.class);
            log.info("Parsed response from Druid and found {} datasources", responseArr.size());
            return responseArr;
        } catch (Exception e) {
            log.error("Error while querying druid datasources!", e);
            throw new DruidException(e.getMessage(), e);
        } finally {
            httpGet.releaseConnection();
        }
    }

    /**
     * Check the status of a Druid cluster using the {@code /status} endpoint.
     *
     * @param cluster the cluster to query
     * @return the status code from querying the cluster
     * @throws DruidException if an error occured while contacting the cluster
     */
    public int queryDruidClusterStatus(DruidCluster cluster) throws DruidException {
        log.info("Calling Druid broker for status.");
        String url = cluster.getBaseUrl() + DruidConstants.STATUS;
        HttpGet httpGet = newHttpGet(url);
        HttpClient client = newHttpClient(300, 0);
        try {
            HttpResponse response = client.execute(httpGet);
            return response.getStatusLine().getStatusCode();
        } catch (IOException e) {
            throw new DruidException(e.getMessage(), e);
        } finally {
            httpGet.releaseConnection();
        }
    }
}
