/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import io.lettuce.core.RedisFuture;
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.TestUtilities.obtain;
import static com.yahoo.sherlock.store.redis.AbstractLettuceAccessorTest.fakeFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.fail;

@SuppressWarnings("unchecked")
public class LettuceDruidClusterAccessorTest {

    private LettuceDruidClusterAccessor dca;
    private AsyncCommands<String> async;
    private SyncCommands<String> sync;

    private void mocks() {
        dca = mock(LettuceDruidClusterAccessor.class);
        inject(dca, LettuceDruidClusterAccessor.class, "clusterIdName", "id");
        inject(dca, AbstractLettuceAccessor.class, "keyName", "key");
        inject(dca, AbstractLettuceAccessor.class, "mapper", new HashMapper());
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        RedisConnection<byte[]> bin = (RedisConnection<byte[]>) mock(RedisConnection.class);
        async = (AsyncCommands<String>) mock(AsyncCommands.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        AsyncCommands<byte[]> binAsync = (AsyncCommands<byte[]>) mock(AsyncCommands.class);
        when(dca.connect()).thenReturn(conn);
        when(dca.binary()).thenReturn(bin);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
        when(bin.async()).thenReturn(binAsync);
        when(dca.key(anyVararg())).thenCallRealMethod();
        when(dca.unmap(any(), any())).thenCallRealMethod();
        when(dca.map(any())).thenCallRealMethod();
    }

    @Test
    public void testConstructorSetsParameters() {
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.INDEX_CLUSTER_ID, "clusterId");
        LettuceDruidClusterAccessor dca = new LettuceDruidClusterAccessor(params);
        assertEquals("clusterId", obtain(dca, "clusterIdName"));
    }

    private static DruidCluster make(Integer clusterId) {
        DruidCluster cluster = new DruidCluster();
        cluster.setClusterId(clusterId);
        cluster.setBrokerHost("hostname");
        cluster.setBrokerPort(1234);
        cluster.setBrokerEndpoint("druid/v2");
        cluster.setClusterName("name");
        cluster.setHoursOfLag(0);
        cluster.setClusterDescription("");
        return cluster;
    }

    private static Map<String, String> map(DruidCluster cluster) {
        return new HashMapper().map(cluster);
    }

    @Test
    public void testGetDruidCluster() throws IOException, ClusterNotFoundException {
        mocks();
        when(dca.getDruidCluster(anyString())).thenCallRealMethod();
        when(sync.hgetall(anyString())).thenReturn(map(make(1)));
        DruidCluster c = dca.getDruidCluster("1");
        assertEquals(c.getClusterId(), (Integer) 1);
        verify(sync).hgetall(anyString());
    }

    @Test
    public void testGetDruidClusterException() throws IOException, ClusterNotFoundException {
        mocks();
        when(dca.getDruidCluster(anyString())).thenCallRealMethod();
        when(sync.hgetall(anyString())).thenReturn(Collections.emptyMap());
        try {
            dca.getDruidCluster("1");
        } catch (ClusterNotFoundException e) {
            return;
        }
        fail();
    }

    @Test
    public void testPutDruidCluster() throws IOException {
        DruidCluster c = make(null);
        when(dca.newId()).thenReturn(1);
        doCallRealMethod().when(dca).putDruidCluster(any());
        dca.putDruidCluster(c);
        assertEquals(c.getClusterId(), (Integer) 1);
        verify(async).hmset(anyString(), anyMap());
        verify(async).sadd(anyString(), anyString());
    }

    @Test
    public void testDeleteDruidCluster() throws IOException, ClusterNotFoundException {
        mocks();
        doCallRealMethod().when(dca).deleteDruidCluster(anyString());
        when(async.del("key:1")).thenReturn(fakeFuture((long) 0));
        try {
            dca.deleteDruidCluster("1");
        } catch (ClusterNotFoundException e) {
            verify(async).srem(anyString(), anyVararg());
            verify(async).del(anyVararg());
            return;
        }
        fail();
    }

    @Test
    public void testDeleteDruidClusterException()
        throws IOException, ClusterNotFoundException, ExecutionException, InterruptedException {
        mocks();
        doCallRealMethod().when(dca).deleteDruidCluster(anyString());
        RedisFuture<Long> lfuture = mock(RedisFuture.class);
        when(lfuture.get()).thenThrow(new InterruptedException("error"));
        when(async.del(anyString())).thenReturn(lfuture);
        try {
            dca.deleteDruidCluster("1");
        } catch (IOException e) {
            assertEquals("error", e.getMessage());
            return;
        }
        fail();
    }

    @Test
    public void testGetDruidClusterList() throws IOException {
        Set<String> clusterIds = new TreeSet<String>() {
            {
                add("1");
                add("2");
                add("3");
            }
        };
        mocks();
        when(sync.smembers(anyString())).thenReturn(clusterIds);
        for (int i = 1; i <= 3; i++) {
            when(async.hgetall("key:" + i)).thenReturn(fakeFuture(map(make(i))));
        }
        when(dca.getDruidClusterList()).thenCallRealMethod();
        DruidCluster[] clusters = dca.getDruidClusterList().toArray(new DruidCluster[3]);
        DruidCluster[] expected = {make(1), make(2), make(3)};
        assertEqualsNoOrder(expected, clusters);
    }

    @Test
    public void testGetDruidClusterIds() {
        mocks();
        when(dca.getDruidClusterIds()).thenCallRealMethod();
        dca.getDruidClusterIds();
        verify(dca).getDruidClusterIds();
        verify(sync).smembers("id:all");
    }

    @Test
    public void testRemoveFromClusterIdIndex() {
        mocks();
        doCallRealMethod().when(dca).removeFromClusterIdIndex(anyString());
        dca.removeFromClusterIdIndex("2");
        verify(dca).removeFromClusterIdIndex("2");
        verify(sync).srem("id:all", "2");
    }
}
