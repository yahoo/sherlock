/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.lettuce.core.ScoredValue;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.TestUtilities.obtain;
import static com.yahoo.sherlock.store.redis.AbstractLettuceAccessorTest.fakeFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEqualsNoOrder;

@SuppressWarnings("unchecked")
public class LettuceAnomalyReportAccessorTest {

    private LettuceAnomalyReportAccessor ara;
    private AsyncCommands<String> async;
    private AsyncCommands<byte[]> binAsync;

    private static AnomalyReport make(Integer id, String jobId, Integer time, String freq) {
        AnomalyReport a = new AnomalyReport();
        a.setUniqueId(id == null ? null : id.toString());
        a.setJobId(id);
        a.setReportQueryEndTime(time);
        a.setJobFrequency(freq);
        a.setAnomalyTimestamps("1000:1100,1100,1200");
        return a;
    }

    private static Map<String, String> mapify(AnomalyReport r) {
        return new HashMap<String, String>() {
            {
                put("uniqueId", r.getUniqueId());
                put("jobId", r.getJobId().toString());
                put("reportQueryEndTime", r.getReportQueryEndTime().toString());
                put("jobFrequency", r.getJobFrequency());
            }
        };
    }

    private void mocks() {
        ara = mock(LettuceAnomalyReportAccessor.class);
        inject(ara, LettuceAnomalyReportAccessor.class, "jobIdName", "jobId");
        inject(ara, LettuceAnomalyReportAccessor.class, "frequencyName", "freq");
        inject(ara, LettuceAnomalyReportAccessor.class, "timeName", "time");
        inject(ara, LettuceAnomalyReportAccessor.class, "emailIdReportName", "emailIdReportsIndex");
        inject(ara, AbstractLettuceAccessor.class, "keyName", "key");
        inject(ara, AbstractLettuceAccessor.class, "mapper", new HashMapper());
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        RedisConnection<byte[]> bin = (RedisConnection<byte[]>) mock(RedisConnection.class);
        async = (AsyncCommands<String>) mock(AsyncCommands.class);
        SyncCommands<String> sync = (SyncCommands<String>) mock(SyncCommands.class);
        binAsync = (AsyncCommands<byte[]>) mock(AsyncCommands.class);
        when(ara.connect()).thenReturn(conn);
        when(ara.binary()).thenReturn(bin);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
        when(bin.async()).thenReturn(binAsync);
    }

    @Test
    public void testConstructorSetsParameters() {
        StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
        params.put(DatabaseConstants.REDIS_CLUSTERED, "true");
        params.put(DatabaseConstants.DB_NAME, "db_name");
        params.put(DatabaseConstants.ID_NAME, "id_name");
        params.put(DatabaseConstants.REDIS_TIMEOUT, "1500");
        params.put(DatabaseConstants.INDEX_REPORT_JOB_ID, "jobId");
        params.put(DatabaseConstants.INDEX_TIMESTAMP, "timestamp");
        params.put(DatabaseConstants.INDEX_FREQUENCY, "frequency");
        LettuceAnomalyReportAccessor ara = new LettuceAnomalyReportAccessor(params);
        assertEquals("jobId", obtain(ara, "jobIdName"));
        assertEquals("timestamp", obtain(ara, "timeName"));
        assertEquals("frequency", obtain(ara, "frequencyName"));
    }

    @Test
    public void testIsMissingId() {
        AnomalyReport a = new AnomalyReport();
        assertTrue(LettuceAnomalyReportAccessor.isMissingId(a));
    }

    @Test
    public void testPutAnomalyReports() throws IOException {
        List<AnomalyReport> reports = Lists.newArrayList(
                make(1, "100", 1234, "day"),
                make(2, "100", 2345, "day"),
                make(null, "120", 1234, "hour"),
                make(null, "120", 2345, "hour")
        );
        mocks();
        when(ara.newIds(anyInt())).thenReturn(new Integer[] {3, 4});
        doCallRealMethod().when(ara).putAnomalyReports(anyList(), anyList());
        ara.putAnomalyReports(reports, Arrays.asList("aa@email.com"));
        verify(ara).newIds(2);
        verify(async, times(16)).sadd(anyString(), anyString());
        verify(async, times(4)).hmset(anyString(), anyMap());
        verify(binAsync, times(4)).zadd(any(), any());
        verify(ara).awaitRaw(anyCollection());
        // verify reports with no anomaly timestamps
        reports.get(0).setAnomalyTimestamps(null);
        ara.putAnomalyReports(reports, Arrays.asList("aa@email.com", "bb@email.com"));
        verify(ara).newIds(2);
        verify(async, times(36)).sadd(anyString(), anyString());
        verify(async, times(8)).hmset(anyString(), anyMap());
        verify(binAsync, times(7)).zadd(any(), any());
    }

    @Test
    public void testGetAnomalyReportsForJobAndForJobAtTimeAndDeleteReports() throws IOException {
        String jobId = "jobId:2";
        String freq = "freq:day";
        Set<String> resId = Sets.newHashSet("1", "2", "3", "4", "5");
        Set<String> resFreq = Sets.newHashSet("2", "3", "4", "5", "6");
        // 2, 3, 4, 5
        mocks();
        when(async.smembers(jobId)).thenReturn(fakeFuture(resId));
        when(async.smembers(freq)).thenReturn(fakeFuture(resFreq));
        AnomalyReport a1 = make(2, "2", 5000, "day");
        AnomalyReport a2 = make(3, "2", 5000, "day");
        AnomalyReport a3 = make(4, "2", 5000, "day");
        AnomalyReport a4 = make(5, "2", 5000, "day");
        AnomalyReport[] aArr = {a1, a2, a3, a4};
        Map<String, String>[] mArr = new Map[]{mapify(a1), mapify(a2), mapify(a3), mapify(a4)};
        for (int i = 0; i < 4; i++) {
            when(async.hgetall("key:" + aArr[i].getUniqueId())).thenReturn(fakeFuture(mArr[i]));
        }
        List<ScoredValue<byte[]>> common = Lists.newArrayList(
            ScoredValue.fromNullable(0.0, new byte[]{12, 34}),
            ScoredValue.fromNullable(1.0, new byte[]{23, 45}),
            ScoredValue.fromNullable(2.0, new byte[]{34, 56}),
            ScoredValue.fromNullable(3.0, new byte[]{45, 67})
        );
        List<ScoredValue<byte[]>> end = Lists.newArrayList(
            ScoredValue.fromNullable(2.0, new byte[]{55, 55})
        );
        int[] iPtr = {0};
        when(binAsync.zrangeWithScores(any(byte[].class), anyLong(), anyLong()))
                .thenAnswer(iom -> iPtr[0]++ % 2 == 0 ? fakeFuture(common) : fakeFuture(end));
        when(ara.getAnomalyReportsForJob(anyString(), anyString())).thenCallRealMethod();
        when(ara.key(anyVararg())).thenCallRealMethod();
        when(ara.unmap(any(Class.class), anyMap())).thenCallRealMethod();
        List<AnomalyReport> result = ara.getAnomalyReportsForJob("2", "day");
        assertEquals(4, result.size());
        assertEqualsNoOrder(aArr, result.toArray());
        Set<String> timeId = Sets.newHashSet("3", "4", "10", "12");
        // 3, 4
        when(async.smembers("time:5000")).thenReturn(fakeFuture(timeId));
        when(ara.getAnomalyReportsForJobAtTime(anyString(), anyString(), anyString())).thenCallRealMethod();
        result = ara.getAnomalyReportsForJobAtTime("2", "5000", "day");
        assertEquals(2, result.size());
        assertEqualsNoOrder(result.toArray(), new AnomalyReport[]{a2, a3});
        // delete 3,4
        doCallRealMethod().when(ara).deleteAnomalyReportsForJobAtTime(anyString(), anyString(), anyString());
        ara.deleteAnomalyReportsForJobAtTime("2", "5000", "day");
        verify(async, times(6)).srem(anyString(), anyVararg());
        verify(async, times(2)).del(anyVararg());
        verify(binAsync, times(4)).del(anyVararg());
        // delete all
        doCallRealMethod().when(ara).deleteAnomalyReportsForJob(anyString());
        when(async.smembers(jobId)).thenReturn(fakeFuture(Sets.newHashSet("2", "3", "4", "5")));
        ara.deleteAnomalyReportsForJob("2");
        verify(async, times(14)).srem(anyString(), anyVararg());
        verify(async, times(7)).del(anyVararg());
        verify(binAsync, times(12)).del(anyVararg());
        // test getAnomalyReportsForEmailId()
        Set<String> reportIds = Sets.newHashSet("2", "3", "4", "5");
        when(async.smembers(DatabaseConstants.INDEX_EMAILID_REPORT + ":" + "my@email.com")).thenReturn(fakeFuture(reportIds));
        Long numReports = (long) reportIds.size();
        String[] reports = {"2", "3", "4", "5"};
        when(async.srem(DatabaseConstants.INDEX_EMAILID_REPORT + ":" + "my@email.com",  reports)).thenReturn(fakeFuture(numReports));
        doCallRealMethod().when(ara).getAnomalyReportsForEmailId("my@email.com");
        assertEquals(ara.getAnomalyReportsForEmailId("my@email.com").size(), 4);
    }

}
