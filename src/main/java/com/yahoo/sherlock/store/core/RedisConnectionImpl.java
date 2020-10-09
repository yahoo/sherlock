/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.core;

import io.lettuce.core.api.StatefulRedisConnection;

/**
 * This class implements {@code RedisConnection} to wrap a {@code StatefulRedisConnection}.
 *
 * @param <K> Redis primary type
 */
public class RedisConnectionImpl<K> implements RedisConnection<K> {

    private final StatefulRedisConnection<K, K> connection;

    /**
     * @param connection Redis connection to wrap
     */
    protected RedisConnectionImpl(StatefulRedisConnection<K, K> connection) {
        this.connection = connection;
    }

    @Override
    public AsyncCommands<K> async() {
        return new AsyncCommandsImpl<>(connection.async());
    }

    @Override
    public SyncCommands<K> sync() {
        return  new SyncCommandsImpl<>(connection.sync());
    }

    @Override
    public void close() {
        connection.close();
    }
}
