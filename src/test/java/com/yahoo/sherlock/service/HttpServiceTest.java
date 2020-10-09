/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.utils.SHttpClient;
import com.yahoo.sherlock.utils.SSslUtils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

/**
 * Test for HttpService.
 */
public class HttpServiceTest {

    private HttpService httpService;
    private SHttpClient sHttpClient;
    private HttpClient client;
    private HttpResponse res;
    private HttpPost post;
    private HttpGet get;

    private void mocks() {
        httpService = mock(HttpService.class);
        sHttpClient = mock(SHttpClient.class);
        res = mock(HttpResponse.class);
        client = mock(HttpClient.class);
        post = mock(HttpPost.class);
        get = mock(HttpGet.class);
        when(httpService.getHttpClient()).thenReturn(sHttpClient);
    }

    private void mockGets() {
        mocks();
        when(sHttpClient.newHttpGet(anyString())).thenReturn(get);
        when(sHttpClient.newHttpPost(anyString())).thenReturn(post);
        when(sHttpClient.newHttpClient()).thenReturn(client);
        when(sHttpClient.newHttpClient(anyBoolean(), anyString())).thenReturn(client);
        when(sHttpClient.newHttpClient(anyInt(), anyInt(), anyBoolean(), anyString())).thenReturn(client);
    }

    @Test
    public void testNewHttpClientInstance() {
        mocks();
        SSslUtils sSslUtils = new SSslUtils();
        TestUtilities.inject(sHttpClient, SHttpClient.class, "sSslUtils", sSslUtils);
        when(sHttpClient.newHttpClient(anyInt(), anyInt(), anyBoolean(), anyString())).thenCallRealMethod();
        when(sHttpClient.newHttpClient()).thenCallRealMethod();
        HttpClient client = sHttpClient.newHttpClient(1000, 4, false, "");
        assertNotNull(client);
        client = sHttpClient.newHttpClient();
        assertNotNull(client);

        // test client with default SSLContext
        client = sHttpClient.newHttpClient(1000, 4, true, "");
        assertNotNull(client);
        client = sHttpClient.newHttpClient(1000, 4, true, null);
        assertNotNull(client);
        // test client with custom SSLContext
        CLISettings.CUSTOM_SSL_CONTEXT_PROVIDER_CLASS = "com.yahoo.sherlock.utils.SpecificContext";
        CLISettings.KEY_DIR = "src/test/resources";
        CLISettings.CERT_DIR = "src/test/resources";
        client = sHttpClient.newHttpClient(1000, 4, true, "file1");
        assertNotNull(client);
    }

    @Test
    public void testNewPostMethodInstance() {
        mocks();
        when(sHttpClient.newHttpPost(anyString())).thenCallRealMethod();
        String url = "https://www.battlog.battlefield.com/bf3";
        HttpPost post = sHttpClient.newHttpPost(url);
        assertEquals(post.getURI().toString(), url);
    }

    @Test
    public void testNewHttpGet() {
        mocks();
        when(sHttpClient.newHttpGet(anyString())).thenCallRealMethod();
        String url = "https://www.battlog.battlefield.com/bf3";
        HttpGet get = sHttpClient.newHttpGet(url);
        assertEquals(get.getURI().toString(), url);
    }

    @Test
    public void testQueryDruid() throws DruidException, IOException {
        mockGets();
        when(httpService.queryDruid(any(DruidCluster.class), any(JsonObject.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        when(cluster.getBrokerUrl()).thenReturn("localhost:9999/druid/v2");
        JsonObject query = new JsonObject();
        query.addProperty("granularity", "hour");
        query.addProperty("something", "else");
        StatusLine sl = mock(StatusLine.class);
        when(res.getStatusLine()).thenReturn(sl);
        when(sl.getStatusCode()).thenReturn(200);
        when(client.execute(post)).thenReturn(res);
        when(cluster.getPrincipalName()).thenReturn("");
        when(cluster.getIsSSLAuth()).thenReturn(false);
        HttpEntity ent = mock(HttpEntity.class);
        JsonArray arr = new JsonArray();
        arr.add(5);
        arr.add(10);
        arr.add(15);
        String arrJson = new Gson().toJson(arr);
        InputStream is = new ByteArrayInputStream(arrJson.getBytes(StandardCharsets.UTF_8));
        when(ent.getContent()).thenReturn(is);
        when(res.getEntity()).thenReturn(ent);
        JsonArray result = httpService.queryDruid(cluster, query);
        Integer[] resultArr = new Integer[result.size()];
        assertEquals(3, resultArr.length);
        Integer[] expected = {5, 10, 15};
        for (int i = 0; i < 3; i++) {
            resultArr[i] = result.get(i).getAsInt();
        }
        assertEqualsNoOrder(resultArr, expected);
        verify(client, times(1)).execute(any(HttpPost.class));
        verify(sl, times(1)).getStatusCode();
        verify(post, times(1)).releaseConnection();
    }

    @Test
    public void testQueryDruidBadResponse() throws DruidException, IOException {
        mockGets();
        when(httpService.queryDruid(any(DruidCluster.class), any(JsonObject.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        when(cluster.getBrokerUrl()).thenReturn("localhost:9999/druid/v2");
        JsonObject query = new JsonObject();
        query.addProperty("granularity", "hour");
        query.addProperty("something", "else");
        StatusLine sl = mock(StatusLine.class);
        when(res.getStatusLine()).thenReturn(sl);
        when(sl.getStatusCode()).thenReturn(500);
        when(client.execute(post)).thenReturn(res);
        when(cluster.getPrincipalName()).thenReturn("");
        when(cluster.getIsSSLAuth()).thenReturn(false);
        try {
            httpService.queryDruid(cluster, query);
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "Post request to broker endpoint failed: 500");
            verify(post, times(1)).releaseConnection();
            return;
        }
        fail();
    }

    @Test
    public void testQueryDruidException() throws DruidException, IOException {
        mockGets();
        when(httpService.queryDruid(any(DruidCluster.class), any(JsonObject.class))).thenCallRealMethod();
        when(client.execute(any(HttpPost.class))).thenThrow(new IOException("error"));
        DruidCluster cluster = mock(DruidCluster.class);
        when(cluster.getPrincipalName()).thenReturn("");
        when(cluster.getIsSSLAuth()).thenReturn(false);
        try {
            httpService.queryDruid(cluster, new JsonObject());
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "error");
            verify(post, times(1)).releaseConnection();
            return;
        }
        fail();
    }

    @Test
    public void testQueryDruidDatasources() throws DruidException, IOException {
        mockGets();
        when(httpService.queryDruidDatasources(any(DruidCluster.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        String url = "http://battlelog.battlefield.com/bf3/";
        when(cluster.getBrokerUrl()).thenReturn(url);
        StatusLine sl = mock(StatusLine.class);
        when(sl.getStatusCode()).thenReturn(200);
        when(res.getStatusLine()).thenReturn(sl);
        when(client.execute(any(HttpGet.class))).thenReturn(res);
        JsonArray dsarr = new JsonArray();
        dsarr.add("shrek");
        dsarr.add("farquaad");
        dsarr.add("donkey");
        String dsarrStr = new Gson().toJson(dsarr);
        InputStream is = new ByteArrayInputStream(dsarrStr.getBytes(StandardCharsets.UTF_8));
        HttpEntity ent = mock(HttpEntity.class);
        when(ent.getContent()).thenReturn(is);
        when(res.getEntity()).thenReturn(ent);
        JsonArray result = httpService.queryDruidDatasources(cluster);
        String[] resultArr = new String[result.size()];
        assertEquals(3, resultArr.length);
        for (int i = 0; i < 3; i++) {
            resultArr[i] = result.get(i).getAsString();
        }
        String[] expected = {"shrek", "farquaad", "donkey"};
        assertEqualsNoOrder(expected, resultArr);
        verify(get, times(1)).releaseConnection();
        verify(client, times(1)).execute(any(HttpGet.class));
    }

    @Test
    public void testQueryDruidDatasourcesBadResponse() throws DruidException, IOException {
        mockGets();
        when(httpService.queryDruidDatasources(any(DruidCluster.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        String url = "http://battlelog.battlefield.com/bf3/";
        when(cluster.getBrokerUrl()).thenReturn(url);
        StatusLine sl = mock(StatusLine.class);
        when(sl.getStatusCode()).thenReturn(500);
        when(res.getStatusLine()).thenReturn(sl);
        when(client.execute(any(HttpGet.class))).thenReturn(res);
        try {
            httpService.queryDruidDatasources(cluster);
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "Get request to cluster datasources endpoint failed: 500");
            verify(get, times(1)).releaseConnection();
            return;
        }
        fail();
    }

    @Test
    public void testQueryDruidDatasourcesException() throws DruidException, IOException {
        mockGets();
        when(httpService.queryDruidDatasources(any(DruidCluster.class))).thenCallRealMethod();
        when(client.execute(any(HttpGet.class))).thenThrow(new IOException("error"));
        try {
            httpService.queryDruidDatasources(mock(DruidCluster.class));
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "error");
            verify(get, times(1)).releaseConnection();
            return;
        }
        fail();
    }

    @Test
    public void testQueryDruidStatus() throws DruidException, IOException {
        mockGets();
        String url = "http://battlelog.battlefield.com/bf4";
        DruidCluster cluster = mock(DruidCluster.class);
        when(cluster.getBrokerUrl()).thenReturn(url);
        when(client.execute(any(HttpGet.class))).thenReturn(res);
        StatusLine sl = mock(StatusLine.class);
        when(sl.getStatusCode()).thenReturn(200);
        when(res.getStatusLine()).thenReturn(sl);
        when(httpService.queryDruidClusterStatus(any(DruidCluster.class))).thenCallRealMethod();
        int result = httpService.queryDruidClusterStatus(cluster);
        assertEquals(result, 200);
        verify(get, times(1)).releaseConnection();
        when(sl.getStatusCode()).thenReturn(500);
        result = httpService.queryDruidClusterStatus(cluster);
        assertEquals(result, 500);
        when(client.execute(any(HttpGet.class))).thenThrow(new IOException("error"));
        try {
            httpService.queryDruidClusterStatus(cluster);
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "error");
            verify(get, times(3)).releaseConnection();
            return;
        }
        fail();
    }

}
