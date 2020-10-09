/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.core;

/**
 * This interface wraps either a {@code StatefulRedisConnection} or a {@code StatefulRedisClusterConnection}
 * to provide code reusability in the accessors.
 *
 * @param <K> primary Redis type
 */
public interface RedisConnection<K> extends AutoCloseable {

    /**
     * @return an asynchronous command set
     */
    AsyncCommands<K> async();

    /**
     * @return a synchronous command set
     */
    SyncCommands<K> sync();

    @Override
    void close();
}
