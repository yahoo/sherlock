/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.yahoo.sherlock.exception.DetectorServiceException;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.DruidConstants;
import com.yahoo.sherlock.utils.SHttpClient;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Service class for http rest calls.
 */
@Slf4j
public class HttpService {

    /**
     * Method to get SHttpClient instance.
     * @return SHttpClient instance
     */
    public SHttpClient getHttpClient() {
        return SHttpClient.getSHttpClient();
    }

    /**
     *
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
        HttpClient client = getHttpClient().newHttpClient(cluster.getIsSSLAuth(), cluster.getPrincipalName());
        HttpPost httpPost = getHttpClient().newHttpPost(url);
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
        HttpClient client = getHttpClient().newHttpClient(2000, 2, cluster.getIsSSLAuth(), cluster.getPrincipalName());
        HttpGet httpGet = getHttpClient().newHttpGet(url);
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
        HttpGet httpGet = getHttpClient().newHttpGet(url);
        HttpClient client = getHttpClient().newHttpClient(300, 0, cluster.getIsSSLAuth(), cluster.getPrincipalName());
        try {
            HttpResponse response = client.execute(httpGet);
            return response.getStatusLine().getStatusCode();
        } catch (IOException e) {
            throw new DruidException(e.getMessage(), e);
        } finally {
            httpGet.releaseConnection();
        }
    }

    /**
     *
     * Service method to query the Prophet Service and get the response.
     *
     * @param prophetUrl   full url of the Prophet Service
     * @param prophetQuery Prophet query json object
     * @return Prophet response as a json array
     * @throws DetectorServiceException http request exception while querying Prophet
     */
    public JsonObject queryProphetService(String prophetUrl, JsonObject prophetQuery) throws DetectorServiceException {
        log.info("Calling Prophet REST Service.");
        HttpPost httpPost = getHttpClient().newHttpPost(prophetUrl);
        HttpClient client = getHttpClient().newHttpClient(CLISettings.PROPHET_TIMEOUT, 0, false, CLISettings.PROPHET_PRINCIPAL);
        try {
            HttpEntity httpEntity = new StringEntity(prophetQuery.toString(), ContentType.APPLICATION_JSON);
            httpPost.setEntity(httpEntity);
            // execute query to Prophet Service
            HttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error(("Post request to Prophet REST endpoint failed: {}"), response.getStatusLine());
                throw new DetectorServiceException("Prophet Rest endpoint failed with HTTP Status " + statusCode);
            }
            // read the response body.
            InputStream inputStream = response.getEntity().getContent();
            log.info("Retrieved response from Prophet Service.");
            Gson gson = new Gson();
            // get the response as json array
            JsonObject jsonObject = gson.fromJson(new InputStreamReader(inputStream), JsonObject.class);
            return jsonObject;
        } catch (Exception e) {
            log.error("Error while sending Prophet Query!", e);
            throw new DetectorServiceException(e.getMessage(), e);
        } finally {
            httpPost.releaseConnection();
        }
    }
}
