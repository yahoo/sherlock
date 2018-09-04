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

    private HttpService http;
    private HttpClient client;
    private HttpResponse res;
    private HttpPost post;
    private HttpGet get;

    private void mocks() {
        http = mock(HttpService.class);
        res = mock(HttpResponse.class);
        client = mock(HttpClient.class);
        post = mock(HttpPost.class);
        get = mock(HttpGet.class);
    }

    private void mockGets() {
        mocks();
        when(http.newHttpGet(anyString())).thenReturn(get);
        when(http.newHttpPost(anyString())).thenReturn(post);
        when(http.newHttpClient()).thenReturn(client);
        when(http.newHttpClient(anyInt(), anyInt())).thenReturn(client);
    }

    @Test
    public void testNewHttpClientInstance() {
        mocks();
        when(http.newHttpClient(anyInt(), anyInt())).thenCallRealMethod();
        when(http.newHttpClient()).thenCallRealMethod();
        HttpClient client = http.newHttpClient(1000, 4);
        assertNotNull(client);
        client = http.newHttpClient();
        assertNotNull(client);
    }

    @Test
    public void testNewPostMethodInstance() {
        mocks();
        when(http.newHttpPost(anyString())).thenCallRealMethod();
        String url = "https://www.battlog.battlefield.com/bf3";
        HttpPost post = http.newHttpPost(url);
        assertEquals(post.getURI().toString(), url);
    }

    @Test
    public void testNewHttpGet() {
        mocks();
        when(http.newHttpGet(anyString())).thenCallRealMethod();
        String url = "https://www.battlog.battlefield.com/bf3";
        HttpGet get = http.newHttpGet(url);
        assertEquals(get.getURI().toString(), url);
    }

    @Test
    public void testQueryDruid() throws DruidException, IOException {
        mockGets();
        when(http.queryDruid(any(DruidCluster.class), any(JsonObject.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        when(cluster.getBrokerUrl()).thenReturn("localhost:9999/druid/v2");
        JsonObject query = new JsonObject();
        query.addProperty("granularity", "hour");
        query.addProperty("something", "else");
        StatusLine sl = mock(StatusLine.class);
        when(res.getStatusLine()).thenReturn(sl);
        when(sl.getStatusCode()).thenReturn(200);
        when(client.execute(post)).thenReturn(res);
        HttpEntity ent = mock(HttpEntity.class);
        JsonArray arr = new JsonArray();
        arr.add(5);
        arr.add(10);
        arr.add(15);
        String arrJson = new Gson().toJson(arr);
        InputStream is = new ByteArrayInputStream(arrJson.getBytes(StandardCharsets.UTF_8));
        when(ent.getContent()).thenReturn(is);
        when(res.getEntity()).thenReturn(ent);
        JsonArray result = http.queryDruid(cluster, query);
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
        when(http.queryDruid(any(DruidCluster.class), any(JsonObject.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        when(cluster.getBrokerUrl()).thenReturn("localhost:9999/druid/v2");
        JsonObject query = new JsonObject();
        query.addProperty("granularity", "hour");
        query.addProperty("something", "else");
        StatusLine sl = mock(StatusLine.class);
        when(res.getStatusLine()).thenReturn(sl);
        when(sl.getStatusCode()).thenReturn(500);
        when(client.execute(post)).thenReturn(res);
        try {
            http.queryDruid(cluster, query);
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
        when(http.queryDruid(any(DruidCluster.class), any(JsonObject.class))).thenCallRealMethod();
        when(client.execute(any(HttpPost.class))).thenThrow(new IOException("error"));
        try {
            http.queryDruid(mock(DruidCluster.class), new JsonObject());
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
        when(http.queryDruidDatasources(any(DruidCluster.class))).thenCallRealMethod();
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
        JsonArray result = http.queryDruidDatasources(cluster);
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
        when(http.queryDruidDatasources(any(DruidCluster.class))).thenCallRealMethod();
        DruidCluster cluster = mock(DruidCluster.class);
        String url = "http://battlelog.battlefield.com/bf3/";
        when(cluster.getBrokerUrl()).thenReturn(url);
        StatusLine sl = mock(StatusLine.class);
        when(sl.getStatusCode()).thenReturn(500);
        when(res.getStatusLine()).thenReturn(sl);
        when(client.execute(any(HttpGet.class))).thenReturn(res);
        try {
            http.queryDruidDatasources(cluster);
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
        when(http.queryDruidDatasources(any(DruidCluster.class))).thenCallRealMethod();
        when(client.execute(any(HttpGet.class))).thenThrow(new IOException("error"));
        try {
            http.queryDruidDatasources(mock(DruidCluster.class));
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
        when(http.queryDruidClusterStatus(any(DruidCluster.class))).thenCallRealMethod();
        int result = http.queryDruidClusterStatus(cluster);
        assertEquals(result, 200);
        verify(get, times(1)).releaseConnection();
        when(sl.getStatusCode()).thenReturn(500);
        result = http.queryDruidClusterStatus(cluster);
        assertEquals(result, 500);
        when(client.execute(any(HttpGet.class))).thenThrow(new IOException("error"));
        try {
            http.queryDruidClusterStatus(cluster);
        } catch (DruidException e) {
            assertEquals(e.getMessage(), "error");
            verify(get, times(3)).releaseConnection();
            return;
        }
        fail();
    }

    @Test
    public void testQueryDruidStatusOkString() throws IOException, DruidException
    {
        mocks();
        when(http.queryDruidClusterStatus(any(DruidCluster.class))).thenReturn(200);
        when(http.queryDruidClusterStatusString(any(DruidCluster.class))).thenCallRealMethod();
        String okResult = http.queryDruidClusterStatusString(null);
        assertEquals(okResult, "OK");
    }

    @Test
    public void testQueryDruidStatusErrorString() throws IOException, DruidException
    {
        mocks();
        when(http.queryDruidClusterStatus(any(DruidCluster.class))).thenThrow(new RuntimeException());
        when(http.queryDruidClusterStatusString(any(DruidCluster.class))).thenCallRealMethod();
        String errorResult = http.queryDruidClusterStatusString(null);
        assertEquals(errorResult, "ERROR");
    }

}
