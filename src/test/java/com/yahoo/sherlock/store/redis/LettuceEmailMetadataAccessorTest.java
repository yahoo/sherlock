/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.google.common.collect.Sets;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.sherlock.TestUtilities.inject;
import static com.yahoo.sherlock.store.redis.AbstractLettuceAccessorTest.fakeFuture;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

/**
 * Test for LettuceEmailMetadataAccessor.
 */
public class LettuceEmailMetadataAccessorTest {

    private LettuceEmailMetadataAccessor ema;
    private AsyncCommands<String> async;
    private SyncCommands<String> sync;
    private EmailMetaData emailMetaData;

    private void mocks() {
        ema = mock(LettuceEmailMetadataAccessor.class);
        inject(ema, LettuceEmailMetadataAccessor.class, "emailIdIndex", "emailIdIndex");
        inject(ema, LettuceEmailMetadataAccessor.class, "emailTriggerIndex", "emailTriggerIndex");
        inject(ema, LettuceEmailMetadataAccessor.class, "emailJobIndex", "emailJobIndex");
        inject(ema, AbstractLettuceAccessor.class, "keyName", "key");
        inject(ema, AbstractLettuceAccessor.class, "mapper", new HashMapper());
        RedisConnection<String> conn = (RedisConnection<String>) mock(RedisConnection.class);
        RedisConnection<byte[]> bin = (RedisConnection<byte[]>) mock(RedisConnection.class);
        async = (AsyncCommands<String>) mock(AsyncCommands.class);
        sync = (SyncCommands<String>) mock(SyncCommands.class);
        AsyncCommands<byte[]> binAsync = (AsyncCommands<byte[]>) mock(AsyncCommands.class);
        when(ema.connect()).thenReturn(conn);
        when(ema.binary()).thenReturn(bin);
        when(conn.sync()).thenReturn(sync);
        when(conn.async()).thenReturn(async);
        when(bin.async()).thenReturn(binAsync);
        when(ema.key(anyVararg())).thenCallRealMethod();
        when(ema.unmap(any(), any())).thenCallRealMethod();
        when(ema.map(any())).thenCallRealMethod();
    }

    private static Map<String, String> map(EmailMetaData emailMetaData) {
        return new HashMapper().map(emailMetaData);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        emailMetaData = new EmailMetaData();
        emailMetaData.setEmailId("my@email.com");
        emailMetaData.setSendOutHour("14");
        emailMetaData.setSendOutMinute("19");
        emailMetaData.setRepeatInterval(Triggers.INSTANT.toString());
    }

    @Test
    public void testPutEmailMetadata() throws Exception {
        mocks();
        doCallRealMethod().when(ema).putEmailMetadata(any(EmailMetaData.class));
        ema.putEmailMetadata(emailMetaData);
        verify(async).hmset(anyString(), anyMap());
        verify(async, times(2)).sadd(anyString(), anyVararg());
        emailMetaData.setRepeatInterval(Triggers.DAY.toString());
        ema.putEmailMetadata(emailMetaData);
        verify(async, times(2)).hmset(anyString(), anyMap());
        verify(async, times(4)).sadd(anyString(), anyVararg());
    }

    @Test
    public void testPutEmailMetadataIfNotExist() throws Exception {
        mocks();
        doCallRealMethod().when(ema).putEmailMetadataIfNotExist(anyString(), anyString());
        // if email is present
        when(async.sadd("emailIdIndex:Emails", emailMetaData.getEmailId())).thenReturn(fakeFuture(Long.valueOf("0")));
        ema.putEmailMetadataIfNotExist(emailMetaData.getEmailId(), "1");
        verify(ema, times(0)).putEmailMetadata(any(EmailMetaData.class));
        verify(async, times(1)).sadd("emailIdIndex:Emails", emailMetaData.getEmailId());
        verify(async, times(1)).sadd("emailJobIndex:" + emailMetaData.getEmailId(), "1");
        // if email is not present
        when(async.sadd("emailIdIndex:Emails", emailMetaData.getEmailId())).thenReturn(fakeFuture(Long.valueOf("1")));
        ema.putEmailMetadataIfNotExist(emailMetaData.getEmailId(), "2");
        verify(ema, times(1)).putEmailMetadata(any(EmailMetaData.class));
        verify(async, times(4)).sadd(anyString(), anyString());
    }

    @Test
    public void testGetAllEmailMetadata() throws Exception {
        mocks();
        doCallRealMethod().when(ema).getAllEmailMetadata();
        Set<String> emails = Sets.newHashSet("my@email.com", "1@email.com");
        when(async.smembers("emailIdIndex:Emails")).thenReturn(fakeFuture(emails));
        when(async.hgetall(anyString())).thenReturn(fakeFuture(map(emailMetaData)));
        assertEquals(2, ema.getAllEmailMetadata().size());
    }

    @Test
    public void testGetEmailMetadata() throws Exception {
        mocks();
        doCallRealMethod().when(ema).getEmailMetadata(anyString());
        when(async.hgetall("key:" + emailMetaData.getEmailId())).thenReturn(fakeFuture(map(emailMetaData)));
        EmailMetaData edata = ema.getEmailMetadata(emailMetaData.getEmailId());
        assertEquals(emailMetaData.getEmailId(), edata.getEmailId());
        assertEquals(emailMetaData, edata);
    }

    @Test
    public void testCheckEmailsInInstantIndex() throws Exception {
        mocks();
        doCallRealMethod().when(ema).checkEmailsInInstantIndex(anyList());
        Set<String> emails = Sets.newHashSet("my@email.com", "1@email.com");
        when(async.smembers("emailTriggerIndex:instant")).thenReturn(fakeFuture(emails));
        List<String> remainEmail = ema.checkEmailsInInstantIndex(Arrays.asList("my@email.com"));
        assertEquals(Arrays.asList("my@email.com"), remainEmail);
    }

    @Test
    public void testRemoveFromTriggerIndex() throws Exception {
        mocks();
        doCallRealMethod().when(ema).removeFromTriggerIndex(anyString(), anyString());
        Long l = 0L;
        when(async.srem("emailTriggerIndex:day", "my@email.com")).thenReturn(fakeFuture(l));
        ema.removeFromTriggerIndex("my@email.com", "day");
        verify(async, times(1)).srem("emailTriggerIndex:day", "my@email.com");
    }

    @Test
    public void testGetAllEmailMetadataByTrigger() throws IOException {
        mocks();
        doCallRealMethod().when(ema).getAllEmailMetadataByTrigger(anyString());
        Set<String> emails = Sets.newHashSet("my@email.com", "1@email.com");
        when(async.smembers("emailTriggerIndex:hour")).thenReturn(fakeFuture(emails));
        when(async.hgetall(anyString())).thenReturn(fakeFuture(map(emailMetaData)));
        assertEquals(2, ema.getAllEmailMetadataByTrigger("hour").size());
    }
}
