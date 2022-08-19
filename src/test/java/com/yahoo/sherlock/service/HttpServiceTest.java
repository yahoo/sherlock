/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.DetectorServiceException;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;
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
        verify(get, times(2)).releaseConnection();
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

    /**
     * Test queryProphetService() method handles the IOException.
     * @throws DetectorServiceException the DetectorService Exception
     * @throws IOException IOException
     */
    @Test
    public void testQueryProphetIOException() throws DetectorServiceException, IOException {
        mockGets();
        when(httpService.queryProphetService(anyString(), any(JsonObject.class))).thenCallRealMethod();
        when(client.execute(any(HttpPost.class))).thenThrow(new IOException("IOError"));
        ProphetAPIService prophetAPIService = new ProphetAPIService();
        String prophetUrl = prophetAPIService.generateProphetURL();
        try {
            httpService.queryProphetService(prophetUrl, new JsonObject());
        } catch (DetectorServiceException e) {
            assertEquals(e.getMessage(), "IOError");
            verify(post, times(1)).releaseConnection();
            return;
        }
        fail();
    }

    /**
     * Test queryProphetService() method handles various HTTP Exceptions (HTTP Status 400, 422, 500).
     * @throws DetectorServiceException the DetectorService Exception
     * @throws IOException IOException
     */
    @Test
    public void testQueryProphetDetectorServiceException() throws DetectorServiceException, IOException {
        mockGets();
        ProphetAPIService prophetAPIService = new ProphetAPIService();
        String prophetUrl = prophetAPIService.generateProphetURL();
        when(httpService.queryProphetService(anyString(), any(JsonObject.class))).thenCallRealMethod();
        when(client.execute(any(HttpPost.class))).thenReturn(res);
        StatusLine sl = mock(StatusLine.class);
        when(sl.getStatusCode()).thenReturn(400); // 400 Bad Request
        when(res.getStatusLine()).thenReturn(sl);
        try {
            httpService.queryProphetService(prophetUrl, new JsonObject());
        } catch (DetectorServiceException e) {
            assertEquals(e.getMessage(), "Prophet Rest endpoint failed with HTTP Status 400");
            verify(post, times(1)).releaseConnection();
        }
        when(sl.getStatusCode()).thenReturn(422);
        try {
            httpService.queryProphetService(prophetUrl, new JsonObject());
        } catch (DetectorServiceException e) {
            assertEquals(e.getMessage(), "Prophet Rest endpoint failed with HTTP Status 422");
            verify(post, times(2)).releaseConnection();
        }
        when(sl.getStatusCode()).thenReturn(500);
        try {
            httpService.queryProphetService(prophetUrl, new JsonObject());
        } catch (DetectorServiceException e) {
            assertEquals(e.getMessage(), "Prophet Rest endpoint failed with HTTP Status 500");
            verify(post, times(3)).releaseConnection();
            return;
        }
        fail();
    }

    /**
     * Test queryProphetService() method when Prophet Microservice returns Status code 200
     * and returns forecasted json object.
     * @throws DetectorServiceException the DetectorService Exception
     * @throws IOException IOException
     */
    @Test
    public void testQueryProphetDetectorSuccess() throws DetectorServiceException, IOException {
        mockGets();
        when(httpService.queryProphetService(anyString(), any(JsonObject.class))).thenCallRealMethod();
        ProphetAPIService prophetAPIService = new ProphetAPIService();
        String prophetUrl = prophetAPIService.generateProphetURL();
        // init Prophet Query object
        JsonObject query = new JsonObject();
        query.addProperty("growth", "linear");
        JsonObject tsObject = new JsonObject();
        tsObject.addProperty("1412038800", 80273608.047961);
        tsObject.addProperty("1412042400", 78275518.2733113);
        tsObject.addProperty("1412046000", 65171461.6414272);
        query.add("timeseries", new Gson().toJsonTree(tsObject));

        HttpEntity ent = mock(HttpEntity.class);
        when(client.execute(post)).thenReturn(res);
        StatusLine sl = mock(StatusLine.class);
        when(res.getStatusLine()).thenReturn(sl);
        when(sl.getStatusCode()).thenReturn(200);
        JsonObject obj = new JsonObject();
        obj.addProperty("1412038800", 84239140.95351946);
        obj.addProperty("1412042400", 73508672.07613184);
        obj.addProperty("1412046000", 62778203.10910882);
        String objJson = new Gson().toJson(obj);
        InputStream is = new ByteArrayInputStream(objJson.getBytes(StandardCharsets.UTF_8));
        when(ent.getContent()).thenReturn(is);
        when(res.getEntity()).thenReturn(ent);
        JsonObject result = httpService.queryProphetService(prophetUrl, query);
        assertTrue(result.isJsonObject());
        JsonParser parser = new JsonParser();
        String expected = "{ \"1412038800\": 8.423914095351946E7, \"1412042400\": 7.350867207613184E7, \"1412046000\": 6.277820310910882E7 }";
        assertEquals(parser.parse(expected), result);
        verify(client, times(1)).execute(any(HttpPost.class));
        verify(sl, times(1)).getStatusCode();
        verify(post, times(1)).releaseConnection();
    }
}
