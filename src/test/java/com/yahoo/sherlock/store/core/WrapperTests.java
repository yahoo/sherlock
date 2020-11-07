/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.core;

import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import com.yahoo.sherlock.exception.StoreException;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import org.testng.annotations.Test;

import java.util.Map;

import static com.yahoo.sherlock.TestUtilities.ONCE;
import static com.yahoo.sherlock.TestUtilities.inject;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class WrapperTests {

    @Test
    public void testAsyncCommandsClusterImplCallsWrapperFunctions() {
        @SuppressWarnings("unchecked")
        RedisClusterAsyncCommands<String, String> wrapped = (RedisClusterAsyncCommands<String, String>)
                mock(RedisClusterAsyncCommands.class);
        AsyncCommands<String> cmd = new AsyncCommandsClusterImpl<>(wrapped);
        cmd.setAutoFlushCommands(true);
        verify(wrapped, ONCE).setAutoFlushCommands(true);
        cmd.flushCommands();
        verify(wrapped).flushCommands();
        cmd.keys("pattern:*");
        verify(wrapped).keys("pattern:*");
        cmd.get("key");
        verify(wrapped).get("key");
        cmd.incr("key");
        verify(wrapped).incr("key");
        cmd.sadd("key", "v1", "v2", "v3");
        verify(wrapped).sadd("key", "v1", "v2", "v3");
        cmd.srem("key", "v1", "v2");
        verify(wrapped).srem("key", "v1", "v2");
        cmd.del("key1", "key2");
        verify(wrapped).del("key1", "key2");
        cmd.smembers("key");
        verify(wrapped).smembers("key");
        @SuppressWarnings("unchecked")
        Map<String, String> mockMap = (Map<String, String>) mock(Map.class);
        cmd.hmset("key", mockMap);
        verify(wrapped).hmset("key", mockMap);
        cmd.hgetall("key");
        verify(wrapped).hgetall("key");
        ScoredValue<String> sv1 = ScoredValue.fromNullable(1.0, "12");
        ScoredValue<String> sv2 = ScoredValue.fromNullable(1.2, "13");
        cmd.zadd("key", sv1, sv2);
        verify(wrapped).zadd("key", sv1, sv2);
        cmd.zrangeWithScores("key", 1, 100);
        verify(wrapped).zrangeWithScores("key", 1, 100);
    }

    @Test
    public void testAsyncCommandsImplCallsWrapperFunctions() {
        @SuppressWarnings("unchecked")
        RedisAsyncCommands<String, String> wrapped = (RedisAsyncCommands<String, String>)
                mock(RedisAsyncCommands.class);
        AsyncCommands<String> cmd = new AsyncCommandsImpl<>(wrapped);
        cmd.setAutoFlushCommands(true);
        verify(wrapped, ONCE).setAutoFlushCommands(true);
        cmd.flushCommands();
        verify(wrapped).flushCommands();
        cmd.keys("pattern:*");
        verify(wrapped).keys("pattern:*");
        cmd.get("key");
        verify(wrapped).get("key");
        cmd.incr("key");
        verify(wrapped).incr("key");
        cmd.sadd("key", "v1", "v2", "v3");
        verify(wrapped).sadd("key", "v1", "v2", "v3");
        cmd.srem("key", "v1", "v2");
        verify(wrapped).srem("key", "v1", "v2");
        cmd.del("key1", "key2");
        verify(wrapped).del("key1", "key2");
        cmd.smembers("key");
        verify(wrapped).smembers("key");
        @SuppressWarnings("unchecked")
        Map<String, String> mockMap = (Map<String, String>) mock(Map.class);
        cmd.hmset("key", mockMap);
        verify(wrapped).hmset("key", mockMap);
        cmd.hgetall("key");
        verify(wrapped).hgetall("key");
        ScoredValue<String> sv1 = ScoredValue.fromNullable(1.0, "12");
        ScoredValue<String> sv2 = ScoredValue.fromNullable(1.2, "13");
        cmd.zadd("key", sv1, sv2);
        verify(wrapped).zadd("key", sv1, sv2);
        cmd.zrangeWithScores("key", 1, 100);
        verify(wrapped).zrangeWithScores("key", 1, 100);
    }

    @Test
    public void testSyncCommandsClusterImplCallsWrapperFunctions() {
        @SuppressWarnings("unchecked")
        RedisClusterCommands<String, String> wrapped = (RedisClusterCommands<String, String>)
                mock(RedisClusterCommands.class);
        SyncCommands<String> cmd = new SyncCommandsClusterImpl<>(wrapped);
        cmd.incr("key");
        verify(wrapped).incr("key");
        cmd.smembers("key");
        verify(wrapped).smembers("key");
        cmd.hgetall("key");
        verify(wrapped).hgetall("key");
        cmd.zadd("key", 1.0, "value");
        verify(wrapped).zadd("key", 1.0, "value");
        cmd.zrem("key", "v1", "v2");
        verify(wrapped).zrem("key", "v1", "v2");
        cmd.del("key1", "key2");
        verify(wrapped).del("key1", "key2");
        cmd.zrangeWithScores("key", 0, 100);
        verify(wrapped).zrangeWithScores("key", 0, 100);
        @SuppressWarnings("unchecked")
        Range<Double> range = (Range<Double>) mock(Range.class);
        cmd.zcount("key", range);
        verify(wrapped).zcount("key", range);
        cmd.eval("script", ScriptOutputType.MULTI, new String[]{"key1", "key2"}, "v1", "v2");
        verify(wrapped).eval("script", ScriptOutputType.MULTI, new String[]{"key1", "key2"}, "v1", "v2");
    }

    @Test
    public void testSyncCommandsImplCallsWrapperFunctions() {
        @SuppressWarnings("unchecked")
        RedisCommands<String, String> wrapped = (RedisCommands<String, String>)
                mock(RedisCommands.class);
        SyncCommands<String> cmd = new SyncCommandsImpl<>(wrapped);
        cmd.incr("key");
        verify(wrapped).incr("key");
        cmd.smembers("key");
        verify(wrapped).smembers("key");
        cmd.hgetall("key");
        verify(wrapped).hgetall("key");
        cmd.zadd("key", 1.0, "value");
        verify(wrapped).zadd("key", 1.0, "value");
        cmd.zrem("key", "v1", "v2");
        verify(wrapped).zrem("key", "v1", "v2");
        cmd.del("key1", "key2");
        verify(wrapped).del("key1", "key2");
        cmd.zrangeWithScores("key", 0, 100);
        verify(wrapped).zrangeWithScores("key", 0, 100);
        @SuppressWarnings("unchecked")
        Range<Double> range = (Range<Double>) mock(Range.class);
        cmd.zcount("key", range);
        verify(wrapped).zcount("key", range);
        cmd.eval("script", ScriptOutputType.MULTI, new String[]{"key1", "key2"}, "v1", "v2");
        verify(wrapped).eval("script", ScriptOutputType.MULTI, new String[]{"key1", "key2"}, "v1", "v2");
    }

    @Test
    public void testRedisConnectionClusterImplCallsWrapperFunctions() {
        @SuppressWarnings("unchecked")
        StatefulRedisClusterConnection<String, String> wrapped = (StatefulRedisClusterConnection<String, String>)
                mock(StatefulRedisClusterConnection.class);
        RedisConnection<String> conn = new RedisConnectionClusterImpl<>(wrapped);
        conn.async();
        verify(wrapped).async();
        conn.sync();
        verify(wrapped).sync();
        conn.close();
        verify(wrapped).close();
    }

    @Test
    public void testRedisConnectionImplCallsWrapperFunctions() {
        @SuppressWarnings("unchecked")
        StatefulRedisConnection<String, String> wrapped = (StatefulRedisConnection<String, String>)
                mock(StatefulRedisConnection.class);
        RedisConnection<String> conn = new RedisConnectionImpl<>(wrapped);
        conn.async();
        verify(wrapped).async();
        conn.sync();
        verify(wrapped).sync();
        conn.close();
        verify(wrapped).close();
    }

    @Test
    public void testClientInitializesRedisClients() {
        inject(Client.class, "client", null);
        Client client = Client.get();
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.REDIS_PASSWORD, "password");
        assertNull(client.getRedisClient());
        assertNull(client.getRedisClusterClient());
        client.initializeRedisClient(params);
        client.initializeRedisClusterClient(params);
        client.initializeRedisClient(null);
        client.initializeRedisClusterClient(null);
        assertNotNull(client.getRedisClient());
        assertNotNull(client.getRedisClusterClient());
        client.destroy();
        assertNull(client.getRedisClient());
        assertNull(client.getRedisClusterClient());
        inject(Client.class, "client", null);
    }

    @Test
    public void testClientInitializationWithInvalidHostName() {
        inject(Client.class, "client", null);
        Client client = Client.get();
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.REDIS_HOSTNAME, "");
        try {
            client.initializeRedisClient(params);
        } catch (StoreException e) {
            inject(Client.class, "client", null);
            return;
        }
        inject(Client.class, "client", null);
        fail();
    }

    @Test
    public void testClientInitializationWithInvalidPort() {
        inject(Client.class, "client", null);
        Client client = Client.get();
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.REDIS_PORT, "");
        try {
            client.initializeRedisClient(params);
        } catch (StoreException e) {
            inject(Client.class, "client", null);
            return;
        }
        inject(Client.class, "client", null);
        fail();
    }

    private static class BaseAccessorTestImpl extends BaseAccessor {

        public BaseAccessorTestImpl() {
            super(Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT), false);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBaseAccessorUsesProducer() {
        inject(Client.class, "client", null);
        ConnectionProducer stringProducer = mock(ConnectionProducer.class);
        ConnectionProducer byteProducer = mock(ConnectionProducer.class);
        RedisConnection<String> strConn = (RedisConnection<String>) mock(RedisConnection.class);
        RedisConnection<byte[]> binConn = (RedisConnection<byte[]>) mock(RedisConnection.class);
        when(stringProducer.produce(any())).thenAnswer(iom -> {
                assertTrue(iom.getArguments()[0] instanceof StringCodec);
                return strConn;
            }
        );
        when(byteProducer.produce(any())).thenAnswer(iom -> {
                assertTrue(iom.getArguments()[0] instanceof ByteArrayCodec);
                return binConn;
            }
        );
        BaseAccessor acc = new BaseAccessorTestImpl();
        inject(acc, BaseAccessor.class, "producer", stringProducer);
        assertEquals(strConn, acc.connect());
        verify(stringProducer).produce(any(StringCodec.class));
        inject(acc, BaseAccessor.class, "producer", byteProducer);
        assertEquals(binConn, acc.binary());
        verify(byteProducer).produce(any(ByteArrayCodec.class));
        inject(Client.class, "client", null);
    }

}
