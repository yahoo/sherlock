package com.yahoo.sherlock.store.core;

import com.lambdaworks.redis.codec.RedisCodec;
import com.yahoo.sherlock.store.StoreParams;

/**
 * This class manages a cluster client to produce cluster-based connections.
 */
public class ConnectionProducerClusterImpl implements ConnectionProducer {

    /**
     * @param params Store parameters used to initialize the client
     */
    protected ConnectionProducerClusterImpl(StoreParams params) {
        Client.get().initializeRedisClusterClient(params);
    }

    @Override
    public <K> RedisConnection<K> produce(RedisCodec<K, K> codec) {
        return new RedisConnectionClusterImpl<>(Client.get().getRedisClusterClient().connect(codec));
    }
}
