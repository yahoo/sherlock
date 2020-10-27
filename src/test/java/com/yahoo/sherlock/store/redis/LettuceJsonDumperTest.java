/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static com.yahoo.sherlock.TestUtilities.inject;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for json dump.
 */
public class LettuceJsonDumperTest {

    private LettuceJsonDumper lettuceJsonDumper;
    private Gson gson = new Gson();
    private SyncCommands<String> sync;

    @BeforeMethod
    public void setUp() throws Exception {
        lettuceJsonDumper = mock(LettuceJsonDumper.class);
        inject(lettuceJsonDumper, LettuceJsonDumper.class, "gson", gson);
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        when(lettuceJsonDumper.connect()).thenReturn(conn);
        when(conn.sync()).thenReturn(sync);
    }

    @Test
    public void testWriteRawData() throws Exception {
        doCallRealMethod().when(lettuceJsonDumper).writeRawData(any());
        doNothing().when(lettuceJsonDumper).writeAnomalyTimestampsToRedis(anyMap());
        doNothing().when(lettuceJsonDumper).writeObjectsToRedis(anyMap());
        doNothing().when(lettuceJsonDumper).writeIndexesToRedis(anyMap());
        doNothing().when(lettuceJsonDumper).writeIdsToRedis(anyMap());
        String jsonString = new String(Files.readAllBytes(Paths.get("src/test/resources/redis_json_dump.json")));
        JsonObject jsonObject = new Gson().fromJson(jsonString, JsonObject.class);
        lettuceJsonDumper.writeRawData(jsonObject);
        verify(lettuceJsonDumper, times(1)).writeAnomalyTimestampsToRedis(anyMap());
        verify(lettuceJsonDumper, times(1)).writeIndexesToRedis(anyMap());
        verify(lettuceJsonDumper, times(1)).writeObjectsToRedis(anyMap());
        verify(lettuceJsonDumper, times(1)).writeIdsToRedis(anyMap());
        Set<String> tempSet = new HashSet<>();
        for (String key : jsonObject.keySet()) {
            if (key.contains(DatabaseConstants.ANOMALY_TIMESTAMP)) {
                tempSet.add(key);
            }
        }
        tempSet.stream().forEach(jsonObject::remove);
        lettuceJsonDumper.writeRawData(jsonObject);
        verify(lettuceJsonDumper, times(1)).writeAnomalyTimestampsToRedis(anyMap());
        verify(lettuceJsonDumper, times(2)).writeIndexesToRedis(anyMap());
        verify(lettuceJsonDumper, times(2)).writeObjectsToRedis(anyMap());
        verify(lettuceJsonDumper, times(2)).writeIdsToRedis(anyMap());
    }

    @Test
    public void testClearIndexes() {
        doCallRealMethod().when(lettuceJsonDumper).clearIndexes(anyString(), anyString());
        lettuceJsonDumper.clearIndexes("jobStatusIndex", "RUNNING");
        verify(lettuceJsonDumper).clearIndexes("jobStatusIndex", "RUNNING");
        verify(sync).del("jobStatusIndex:RUNNING");
    }

}
