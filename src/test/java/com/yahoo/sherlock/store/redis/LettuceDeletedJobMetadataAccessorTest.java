package com.yahoo.sherlock.store.redis;

import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.TestUtilities.obtain;
import static com.yahoo.sherlock.store.redis.AbstractLettuceAccessorTest.fakeFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.fail;

@SuppressWarnings("unchecked")
public class LettuceDeletedJobMetadataAccessorTest {

    private LettuceDeletedJobMetadataAccessor ara;
    private AsyncCommands<String> async;
    private SyncCommands<String> sync;

    private void mocks() {
        ara = mock(LettuceDeletedJobMetadataAccessor.class);
        inject(ara, LettuceDeletedJobMetadataAccessor.class, "deletedName", "del");
        inject(ara, AbstractLettuceAccessor.class, "keyName", "key");
        inject(ara, AbstractLettuceAccessor.class, "mapper", new HashMapper());
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        RedisConnection<byte[]> bin = (RedisConnection<byte[]>) mock(RedisConnection.class);
        async = (AsyncCommands<String>) mock(AsyncCommands.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        AsyncCommands<byte[]> binAsync = (AsyncCommands<byte[]>) mock(AsyncCommands.class);
        when(ara.connect()).thenReturn(conn);
        when(ara.binary()).thenReturn(bin);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
        when(bin.async()).thenReturn(binAsync);
        when(ara.key(anyVararg())).thenCallRealMethod();
        when(ara.unmap(any(), any())).thenCallRealMethod();
        when(ara.map(any())).thenCallRealMethod();
    }

    @Test
    public void testConstructorSetsParameters() {
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.INDEX_DELETED_ID, "deleted");
        LettuceDeletedJobMetadataAccessor jma = new LettuceDeletedJobMetadataAccessor(params);
        String deletedName = (String) obtain(jma, "deletedName");
        assertEquals(deletedName, "deleted");
    }

    @Test
    public void testIsMissingId() {
        JobMetadata job = new JobMetadata();
        assertTrue(LettuceDeletedJobMetadataAccessor.isMissingId(job));
        job.setJobId(123);
        assertFalse(LettuceDeletedJobMetadataAccessor.isMissingId(job));
    }

    @Test
    public void testPutJobMetadata() throws IOException {
        mocks();
        JobMetadata job = new JobMetadata();
        job.setJobId(null);
        when(ara.newId()).thenReturn(1000);
        doCallRealMethod().when(ara).putDeletedJobMetadata(any(JobMetadata.class));
        ara.putDeletedJobMetadata(job);
        verify(async).hmset(anyString(), anyMap());
        verify(async).sadd("del:all", "1000");
        assertEquals(job.getJobId(), (Integer) 1000);
    }

    @Test
    public void testGetJobMetadata() throws IOException, JobNotFoundException {
        mocks();
        JobMetadata job = new JobMetadata();
        job.setJobId(1);
        Map<String, String> map = new TreeMap<>();
        map.put("jobId", "1");
        when(sync.hgetall("key:1")).thenReturn(map);
        when(ara.getDeletedJobMetadata(anyString())).thenCallRealMethod();
        assertEquals(job, ara.getDeletedJobMetadata("1"));
    }

    @Test
    public void testGetJobMetadataException() throws IOException, JobNotFoundException {
        mocks();
        when(sync.hgetall(anyString())).thenReturn(Collections.emptyMap());
        when(ara.getDeletedJobMetadata(anyString())).thenCallRealMethod();
        try {
            ara.getDeletedJobMetadata("1");
        } catch (JobNotFoundException e) {
            return;
        }
        fail();
    }

    private static Map<String, String> fakeMap(String jobId) {
        return new TreeMap<String, String>() {
            {
                put("jobId", jobId);
            }
        };
    }

    private static JobMetadata make(Integer jobId) {
        JobMetadata job = new JobMetadata();
        job.setJobId(jobId);
        return job;
    }

    @Test
    public void testGetJobList() throws IOException {
        mocks();
        Set<String> jobIds = new TreeSet<>();
        jobIds.add("1");
        jobIds.add("2");
        jobIds.add("3");
        when(sync.smembers(anyString())).thenReturn(jobIds);
        when(ara.getDeletedJobMetadataList()).thenCallRealMethod();
        for (int i = 1; i <= 3; i++) {
            when(async.hgetall("key:" + i)).thenReturn(fakeFuture(fakeMap(String.valueOf(i))));
        }
        JobMetadata[] result = ara.getDeletedJobMetadataList().toArray(new JobMetadata[3]);
        JobMetadata[] expected = {make(1), make(2), make(3)};
        assertEqualsNoOrder(expected, result);
    }

    @Test
    public void testPutJobMetadataSet() throws IOException {
        mocks();
        doCallRealMethod().when(ara).putDeletedJobMetadata(anyList());
        List<JobMetadata> jobs = new ArrayList<>();
        jobs.add(make(1));
        jobs.add(make(2));
        jobs.add(make(null));
        jobs.add(make(null));
        when(ara.newIds(anyInt())).thenReturn(new Integer[]{3, 4});
        ara.putDeletedJobMetadata(jobs);
        verify(ara).newIds(2);
        for (JobMetadata job : jobs) {
            assertNotNull(job.getJobId());
        }
        verify(async, times(4)).hmset(anyString(), anyMap());
        verify(async, times(4)).sadd(anyString(), anyString());
    }

}
