package com.yahoo.sherlock.store.core;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

/**
 * This class implements {@code RedisConnection} to wrap a {@code StatefulRedisClusterConnection}.
 *
 * @param <K> Redis primary type
 */
public class RedisConnectionClusterImpl<K> implements RedisConnection<K> {

    private final StatefulRedisClusterConnection<K, K> connection;

    /**
     * @param connection cluster connection to wrap
     */
    protected RedisConnectionClusterImpl(StatefulRedisClusterConnection<K, K> connection) {
        this.connection = connection;
    }

    @Override
    public AsyncCommands<K> async() {
        return new AsyncCommandsClusterImpl<>(connection.async());
    }

    @Override
    public SyncCommands<K> sync() {
        return new SyncCommandsClusterImpl<>(connection.sync());
    }

    @Override
    public void close() {
        connection.close();
    }
}
