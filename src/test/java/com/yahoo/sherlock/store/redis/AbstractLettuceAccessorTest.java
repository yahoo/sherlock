/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.beust.jcommander.internal.Lists;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.BaseAccessor;
import com.yahoo.sherlock.store.core.ConnectionProducerClusterImpl;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.TestUtilities.obtain;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class AbstractLettuceAccessorTest {

    private static class TestImpl extends AbstractLettuceAccessor {

        public TestImpl(StoreParams params) {
            super(params);
        }
    }

    private static RedisFuture mockFuture() {
        return mock(RedisFuture.class);
    }

    private static class FakeFuture<T> extends AsyncCommand<T, T, T> {

        private T t;

        private FakeFuture(T t) {
            super(mock(RedisCommand.class));
            this.t = t;
        }

        @Override
        public T get() {
            return t;
        }
    }

    @SuppressWarnings("unchecked")
    protected static <T> RedisFuture<T> fakeFuture(T t) {
        return new FakeFuture<>(t);
    }

    private AbstractLettuceAccessor ala;
    private RedisConnection<String> conn;
    private RedisConnection<byte[]> bin;
    private AsyncCommands<String> async;
    private SyncCommands<String> sync;

    private void mocks() {
        ala = mock(AbstractLettuceAccessor.class);
        conn = (RedisConnection<String>) mock(RedisConnection.class);
        bin = (RedisConnection<byte[]>) mock(RedisConnection.class);
        async = (AsyncCommands<String>) mock(AsyncCommands.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        when(ala.connect()).thenReturn(conn);
        when(ala.binary()).thenReturn(bin);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
    }

    @Test
    public void testConstructorSetsParameters() {
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.REDIS_CLUSTERED, "true");
        params.put(DatabaseConstants.DB_NAME, "db_name");
        params.put(DatabaseConstants.ID_NAME, "id_name");
        params.put(DatabaseConstants.REDIS_TIMEOUT, "1500");
        AbstractLettuceAccessor acc = new TestImpl(params);
        Object producer = obtain(acc, BaseAccessor.class, "producer");
        assertTrue(producer instanceof ConnectionProducerClusterImpl);
        assertNotNull(obtain(acc, AbstractLettuceAccessor.class, "mapper"));
        assertEquals("db_name", obtain(acc, AbstractLettuceAccessor.class, "keyName"));
        assertEquals("id_name", obtain(acc, AbstractLettuceAccessor.class, "idName"));
        assertEquals(1500, obtain(acc, AbstractLettuceAccessor.class, "timeoutMillis"));
    }

    @Test
    public void testAwaitCollectionReordersArrays() {
        RedisFuture[] arr1 = {mockFuture(), mockFuture(), mockFuture()};
        RedisFuture[] arr2 = {mockFuture()};
        RedisFuture[] arr3 = {mockFuture(), mockFuture()};
        RedisFuture[] expected = {
                arr1[0], arr1[1], arr1[2],
                arr2[0],
                arr3[0], arr3[1]
        };
        List<RedisFuture[]> coll = Lists.newArrayList(arr1, arr2, arr3);
        mocks();
        doCallRealMethod().when(ala).awaitCollection(any());
        ala.awaitCollection(coll);
    }

    @Test
    public void testKey() {
        mocks();
        when(ala.key(anyVararg())).thenCallRealMethod();
        inject(ala, AbstractLettuceAccessor.class, "keyName", "key");
        String key = ala.key("abc", "123", "goodbye");
        assertEquals("key:abc:123:goodbye", key);
    }

    @Test
    public void testIndex() {
        mocks();
        String index = AbstractLettuceAccessor.index("abc", "123", "asdf");
        assertEquals("abc:123:asdf", index);
    }

    @Test
    public void testNewId() throws IOException {
        mocks();
        when(sync.incr(anyString())).thenReturn((long) 1234);
        when(ala.newId()).thenCallRealMethod();
        assertEquals((Integer) 1234, ala.newId());
    }

    @Test
    public void testNewIdException() throws IOException {
        mocks();
        when(sync.incr(anyString())).thenThrow(new RedisException("error"));
        when(ala.newId()).thenCallRealMethod();
        try {
            ala.newId();
        } catch (IOException e) {
            assertEquals("error", e.getMessage());
            return;
        }
        fail();
    }

    @Test
    public void testNewIds() throws IOException {
        Long[] lptr = new Long[]{(long) 1};
        mocks();
        when(async.incr(anyString())).thenAnswer(iom -> fakeFuture(lptr[0]++));
        when(ala.newIds(anyInt())).thenCallRealMethod();
        Integer[] ids = ala.newIds(3);
        assertArrayEquals(new Integer[]{1, 2, 3}, ids);
    }

    @Test
    public void testNewIdsException() throws IOException, ExecutionException, InterruptedException {
        mocks();
        RedisFuture<Long> lftr = mock(RedisFuture.class);
        when(lftr.get()).thenThrow(new InterruptedException("error"));
        when(async.incr(anyString())).thenReturn(lftr);
        when(ala.newIds(anyInt())).thenCallRealMethod();
        try {
            ala.newIds(3);
        } catch (IOException e) {
            assertEquals("error", e.getMessage());
            return;
        }
        fail();
    }

}
