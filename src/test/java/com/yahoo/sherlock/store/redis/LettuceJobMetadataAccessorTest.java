package com.yahoo.sherlock.store.redis;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;
import io.lettuce.core.RedisFuture;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DBTestHelper;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.TestUtilities.obtain;
import static com.yahoo.sherlock.store.redis.AbstractLettuceAccessorTest.fakeFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

@SuppressWarnings("unchecked")
public class LettuceJobMetadataAccessorTest {

    private LettuceJobMetadataAccessor jma;
    private DeletedJobMetadataAccessor djma;
    private EmailMetadataAccessor ema;
    private AnomalyReportAccessor ara;
    private AsyncCommands<String> async;
    private SyncCommands<String> sync;

    private void mocks() {
        jma = mock(LettuceJobMetadataAccessor.class);
        djma = mock(DeletedJobMetadataAccessor.class);
        ara = mock(AnomalyReportAccessor.class);
        ema = mock(EmailMetadataAccessor.class);
        inject(jma, LettuceJobMetadataAccessor.class, "anomalyReportAccessor", ara);
        inject(jma, LettuceJobMetadataAccessor.class, "deletedAccessor", djma);
        inject(jma, LettuceJobMetadataAccessor.class, "emailMetadataAccessor", ema);
        inject(jma, LettuceJobMetadataAccessor.class, "jobIdName", "id");
        inject(jma, LettuceJobMetadataAccessor.class, "jobStatusName", "status");
        inject(jma, LettuceJobMetadataAccessor.class, "clusterIdName", "cluster");
        inject(jma, AbstractLettuceAccessor.class, "keyName", "key");
        inject(jma, AbstractLettuceAccessor.class, "mapper", new HashMapper());
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        RedisConnection<byte[]> bin = (RedisConnection<byte[]>) mock(RedisConnection.class);
        async = (AsyncCommands<String>) mock(AsyncCommands.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        AsyncCommands<byte[]> binAsync = (AsyncCommands<byte[]>) mock(AsyncCommands.class);
        when(jma.connect()).thenReturn(conn);
        when(jma.binary()).thenReturn(bin);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
        when(bin.async()).thenReturn(binAsync);
        when(jma.key(anyVararg())).thenCallRealMethod();
        when(jma.unmap(any(), any())).thenCallRealMethod();
        when(jma.map(any())).thenCallRealMethod();
    }

    @Test
    public void testConstructorSetsParameters() {
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.INDEX_JOB_ID, "id");
        params.put(DatabaseConstants.INDEX_JOB_STATUS, "status");
        params.put(DatabaseConstants.INDEX_JOB_CLUSTER_ID, "cluster");
        LettuceJobMetadataAccessor jma = new LettuceJobMetadataAccessor(params);
        assertEquals("id", obtain(jma, "jobIdName"));
        assertEquals("status", obtain(jma, "jobStatusName"));
        assertEquals("cluster", obtain(jma, "clusterIdName"));
    }

    private static JobMetadata make(Integer id, String status, Integer cluster) {
        JobMetadata job = new JobMetadata();
        job.setJobId(id);
        job.setJobStatus(status);
        job.setClusterId(cluster);
        return job;
    }

    private static Map<String, String> map(JobMetadata job) {
        return new HashMapper().map(job);
    }

    @Test
    public void testPerformDeleteJob() throws IOException, JobNotFoundException {
        mocks();
        when(jma.performDeleteJob(anyString())).thenCallRealMethod();
        doNothing().when(ara).deleteAnomalyReportsForJob(anyString());
        when(async.hgetall(anyString())).thenReturn(fakeFuture(map(make(1, "RUNNING", 2))));
        JobMetadata job = jma.performDeleteJob("1");
        verify(async).del(anyVararg());
        verify(async, times(3)).srem(anyString(), anyVararg());
        assertEquals((Integer) 1, job.getJobId());
        assertEquals("RUNNING", job.getJobStatus());
    }

    @Test
    public void testPerformDeleteJobs() throws IOException {
        mocks();
        when(jma.performDeleteJob(anySet())).thenCallRealMethod();
        doNothing().when(ara).deleteAnomalyReportsForJob(anyString());
        Set<String> ids = Sets.newHashSet("1", "2", "3");
        for (int i = 1; i <= 3; i++) {
            RedisFuture<Map<String, String>> ftr = fakeFuture(map(make(i, "RUNNING", 10)));
            when(async.hgetall("key:" + i)).thenReturn(ftr);
        }
        Set<JobMetadata> jobs = jma.performDeleteJob(ids);
        Set<Integer> conIds = new HashSet<>();
        for (JobMetadata job : jobs) {
            conIds.add(job.getJobId());
        }
        assertEquals(3, conIds.size());
        verify(async, times(9)).srem(anyString(), anyVararg());
        verify(async, times(3)).del(anyVararg());
    }

    @Test
    public void testDeleteGivenJobs() throws IOException {
        Set<JobMetadata> jobs = Sets.newHashSet(
                make(1, "RUNNING", 10),
                make(2, "STOPPED", 10),
                make(3, "NODATA", 12)
        );
        mocks();
        doCallRealMethod().when(jma).deleteGivenJobs(anySet());
        jma.deleteGivenJobs(jobs);
        verify(async, times(9)).srem(anyString(), anyVararg());
        verify(async, times(3)).del(anyVararg());
    }

    @Test
    public void testGetJobMetadatas() throws IOException {
        Set<String> ids = Sets.newHashSet("1", "2", "3");
        mocks();
        for (int i = 1; i <= 3; i++) {
            when(async.hgetall("key:" + i)).thenReturn(fakeFuture(map(make(i, "RUNNING", 1))));
        }
        when(jma.getJobMetadata(anySet())).thenCallRealMethod();
        List<JobMetadata> jobs = jma.getJobMetadata(ids);
        Set<Integer> conIds = Sets.newHashSet();
        for (JobMetadata job : jobs) {
            conIds.add(job.getJobId());
        }
        assertEquals(3, conIds.size());
        verify(async, times(3)).hgetall(anyString());
    }

    @Test
    public void testGetJobMetadata() throws IOException, JobNotFoundException {
        mocks();
        when(jma.getJobMetadata(anyString())).thenCallRealMethod();
        when(sync.hgetall("key:1")).thenReturn(map(make(1, "CREATED", 2)));
        JobMetadata job = jma.getJobMetadata("1");
        assertEquals((Integer) 1, job.getJobId());
        assertEquals("CREATED", job.getJobStatus());
        when(sync.hgetall("key:2")).thenReturn(Collections.emptyMap());
        try {
            jma.getJobMetadata("2");
        } catch (JobNotFoundException e) {
            return;
        }
        fail();
    }

    @Test
    public void testPutJobmetadata() throws IOException, JobNotFoundException {
        // missing ID
        mocks();
        doCallRealMethod().when(jma).putJobMetadata(any(JobMetadata.class));
        doNothing().when(ema).putEmailMetadataIfNotExist(anyString(), anyString());
        when(jma.newId()).thenReturn(123);
        JobMetadata job = make(null, "CREATED", 23);
        job.setOwnerEmail("my@email.com");
        jma.putJobMetadata(job);
        assertEquals((Integer) 123, job.getJobId());
        verify(jma).newId();
        verify(async).hmset(anyString(), anyMap());
        verify(ema, times(1)).putEmailMetadataIfNotExist(anyString(), anyString());
        verify(async, times(3)).sadd(anyString(), anyVararg());
        // update
        job.setOwnerEmail("");
        job.setJobStatus("RUNNING");
        jma.putJobMetadata(job);
        verify(async, times(2)).hmset(anyString(), anyMap());
        verify(ema, times(1)).putEmailMetadataIfNotExist(anyString(), anyString());
        verify(async, times(6)).sadd(anyString(), anyVararg());
        verify(jma).newId();
    }

    @Test
    public void testPutJobMetadatas() throws IOException {
        List<JobMetadata> jobs = Lists.newArrayList(
                make(1, "RUNNING", 123),
                make(2, "STOPPED", 123),
                make(null, "CREATED", 1111),
                make(null, "CREATED", 1444)
        );
        mocks();
        doCallRealMethod().when(jma).putJobMetadata(anyList());
        when(jma.newIds(2)).thenReturn(new Integer[]{3, 4});
        jma.putJobMetadata(jobs);
        verify(jma).newIds(2);
        assertEquals(jobs.get(2).getJobId(), (Integer) 3);
        assertEquals(jobs.get(3).getJobId(), (Integer) 4);
        verify(async, times(4)).hmset(anyString(), anyMap());
        verify(async, times(12)).sadd(anyString(), anyVararg());
    }

    @Test
    public void testDeleteJobMetadataWrapper() throws IOException, JobNotFoundException {
        mocks();
        doCallRealMethod().when(jma).deleteJobMetadata(anyString());
        when(jma.performDeleteJob("1")).thenReturn(make(1, null, null));
        jma.deleteJobMetadata("1");
        verify(jma).performDeleteJob("1");
        verify(djma).putDeletedJobMetadata(make(1, null, null));
    }

    @Test
    public void testGetJobMetadataList() throws IOException {
        mocks();
        when(jma.getJobMetadataList()).thenCallRealMethod();
        jma.getJobMetadataList();
        verify(jma).getJobMetadata(anySet());
        verify(sync).smembers("id:all");
    }

    @Test
    public void testGetRunningJobs() throws IOException {
        mocks();
        when(jma.getRunningJobs()).thenCallRealMethod();
        jma.getRunningJobs();
        verify(jma).getJobMetadata(anySet());
        verify(sync).smembers("status:RUNNING");
    }

    @Test
    public void testGetJobsAssociatedWithCluster() throws IOException {
        mocks();
        when(jma.getJobsAssociatedWithCluster(anyString())).thenCallRealMethod();
        jma.getJobsAssociatedWithCluster("1");
        verify(jma).getJobMetadata(anySet());
        verify(sync).smembers("cluster:1");
    }

    @Test
    public void testGetRunningJobsAssociatedWithCluster() throws IOException {
        mocks();
        when(jma.getRunningJobsAssociatedWithCluster(anyString())).thenCallRealMethod();
        when(async.smembers("status:RUNNING")).thenReturn(fakeFuture(Sets.newHashSet("1", "2", "3")));
        when(async.smembers("cluster:1")).thenReturn(fakeFuture(Sets.newHashSet("2", "3")));
        jma.getRunningJobsAssociatedWithCluster("1");
        verify(jma).getJobMetadata(anySet());
    }

    @Test
    public void testDeleteDebugJobs() throws IOException {
        mocks();
        doCallRealMethod().when(jma).deleteDebugJobs();
        jma.deleteDebugJobs();
        verify(jma).performDeleteJob(anySet());
        verify(sync).smembers("status:DEBUG");
    }

    @Test
    public void testDeleteJobs() throws IOException {
        mocks();
        doCallRealMethod().when(jma).deleteJobs(anySet());
        jma.deleteJobs(Collections.emptySet());
        verify(jma).performDeleteJob(anySet());
        verify(djma).putDeletedJobMetadata(anySet());
    }

    @Test
    public void testDeleteEmailFromJobs() throws IOException, JobNotFoundException {
        mocks();
        EmailMetaData emailMetaData = new EmailMetaData("my@gmail.com");
        doCallRealMethod().when(jma).deleteEmailFromJobs(any(EmailMetaData.class));
        Set<String> jobIds = Sets.newHashSet("1", "2");
        JobMetadata jobMetadata1 = DBTestHelper.getNewJob();
        jobMetadata1.setOwnerEmail("my@gmail.com, ab@yahoo.com, xy@sherlock.com");
        jobMetadata1.setJobId(1);
        JobMetadata jobMetadata2 = DBTestHelper.getNewJob();
        jobMetadata2.setOwnerEmail("");
        jobMetadata2.setJobId(2);
        when(jma.getJobMetadata(jobIds)).thenReturn(Arrays.asList(jobMetadata1, jobMetadata2));
        when(async.smembers("emailJobIndex:my@gmail.com")).thenReturn(fakeFuture(jobIds));
        when(async.del("emailJobIndex:my@gmail.com")).thenReturn(fakeFuture(1L));
        doNothing().when(ema).deleteEmailMetadata(any(EmailMetaData.class));
        doNothing().when(jma).putJobMetadata(Arrays.asList(jobMetadata1, jobMetadata2));
        jma.deleteEmailFromJobs(emailMetaData);
        verify(jma, times(1)).putJobMetadata(Arrays.asList(jobMetadata1, jobMetadata2));
        verify(jma, times(1)).getJobMetadata(jobIds);
    }
}
