/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.beust.jcommander.internal.Lists;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.TestUtilities.obtain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class LettuceJobSchedulerTest {

    private LettuceJobScheduler sch;
    private JobMetadataAccessor jma;
    private SyncCommands<String> sync;

    private void mocks() {
        sch = mock(LettuceJobScheduler.class);
        jma = mock(JobMetadataAccessor.class);
        inject(sch, LettuceJobScheduler.class, "jobAccessor", jma);
        inject(sch, LettuceJobScheduler.class, "queueName", "{queue}.job");
        inject(sch, LettuceJobScheduler.class, "pendingQueueName", "{queue}.pending");
        inject(sch, AbstractLettuceAccessor.class, "keyName", "key");
        inject(sch, AbstractLettuceAccessor.class, "mapper", new HashMapper());
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        AsyncCommands<String> async = (AsyncCommands<String>) mock(AsyncCommands.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        when(sch.connect()).thenReturn(conn);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
        when(sch.key(anyVararg())).thenCallRealMethod();
        when(sch.unmap(any(), any())).thenCallRealMethod();
        when(sch.map(any())).thenCallRealMethod();
    }

    @Test
    public void testConstructorSetsParams() {
        StoreParams params = Store.getParamsFor(Store.AccessorType.JOB_SCHEDULER);
        params.put(DatabaseConstants.QUEUE_JOB_SCHEDULE, "queue");
        LettuceJobScheduler js = new LettuceJobScheduler(params);
        assertNotNull(obtain(js, "jobAccessor"));
        assertEquals("{queue}.queue", obtain(js, "queueName"));
        assertEquals("{queue}.queuePending", obtain(js, "pendingQueueName"));
    }

    @Test
    public void testPushQueue() throws IOException {
        mocks();
        doCallRealMethod().when(sch).pushQueue(anyLong(), anyString());
        sch.pushQueue(1000, "1");
        verify(sync).zadd("{queue}.job", 1000.0, "1");
    }

    @Test
    public void testBulkPushQueue() throws IOException {
        mocks();
        doCallRealMethod().when(sch).pushQueue(anyList());
        List<Pair<Integer, String>> pairs = Lists.newArrayList(
                ImmutablePair.of(1000, "1"),
                ImmutablePair.of(2000, "2"),
                ImmutablePair.of(3000, "3")
        );
        sch.pushQueue(pairs);
        verify(sync, times(3)).zadd(anyString(), anyDouble(), anyString());
    }

    @Test
    public void testRemoveQueue() throws IOException, JobNotFoundException {
        mocks();
        doCallRealMethod().when(sch).removeQueue(anyString());
        sch.removeQueue("1");
        verify(sync).zrem("{queue}.job", "1");
    }

    @Test
    public void testBulkRemoveQueue() throws IOException {
        mocks();
        doCallRealMethod().when(sch).removeQueue(anyCollection());
        sch.removeQueue(Lists.newArrayList("1", "2", "3"));
        verify(sync, times(3)).zrem(anyString(), anyString());
    }

    @Test
    public void testRemoveAllQueue() throws IOException {
        mocks();
        doCallRealMethod().when(sch).removeAllQueue();
        sch.removeAllQueue();
        verify(sync).del("{queue}.job", "{queue}.pending");
    }

    @Test
    public void testGetAllQueue() throws IOException {
        mocks();
        when(sch.getAllQueue()).thenCallRealMethod();
        when(sync.zrangeWithScores(anyString(), anyLong(), anyLong())).thenReturn(Lists.newArrayList(
                ScoredValue.fromNullable(1.0, "1"),
                ScoredValue.fromNullable(2.0, "2"),
                ScoredValue.fromNullable(3.0, "3")
        ));
        sch.getAllQueue();
        verify(sync).zrangeWithScores(anyString(), anyLong(), anyLong());
        verify(jma).getJobMetadata(anySet());
    }

    @Test
    public void testPeekQueue() throws IOException {
        mocks();
        when(sch.peekQueue(anyLong())).thenCallRealMethod();
        when(sync.zcount(anyString(), any(Range.class))).thenReturn(5L);
        assertEquals(5, sch.peekQueue(123455));
    }

    @Test
    public void testPopQueueEmpty() throws IOException {
        mocks();
        when(sch.popQueue(anyLong())).thenCallRealMethod();
        when(sync.eval(anyString(), any(ScriptOutputType.class), any(), anyVararg())).thenReturn(Collections.emptyList());
        assertNull(sch.popQueue(123455));
    }

    @Test
    public void testPopQueue() throws IOException, JobNotFoundException {
        mocks();
        when(sch.popQueue(anyLong())).thenCallRealMethod();
        when(sync.eval(anyString(), any(ScriptOutputType.class), any(), anyVararg())).thenReturn(Lists.newArrayList("1"));
        sch.popQueue(1234);
        verify(jma).getJobMetadata("1");
    }

    @Test
    public void testPopQueueNotFound() throws IOException, JobNotFoundException {
        mocks();
        when(sch.popQueue(anyLong())).thenCallRealMethod();
        when(sync.eval(anyString(), any(ScriptOutputType.class), any(), anyVararg())).thenReturn(Lists.newArrayList("1"));
        when(jma.getJobMetadata(anyString())).thenThrow(new JobNotFoundException());
        assertNull(sch.popQueue(1234));
    }

    @Test
    public void testRemovePending() throws IOException {
        mocks();
        doCallRealMethod().when(sch).removePending(anyString());
        sch.removePending("1");
        verify(sync).zrem("{queue}.pending", "1");
    }

    @Test
    public void testBulkRemovePending() throws IOException {
        mocks();
        doCallRealMethod().when(sch).removePending(anyCollection());
        sch.removePending(Lists.newArrayList("1", "2", "3"));
        verify(sync, times(3)).zrem(anyString(), anyString());
    }

}
