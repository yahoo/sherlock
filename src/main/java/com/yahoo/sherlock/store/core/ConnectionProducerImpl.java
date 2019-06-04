package com.yahoo.sherlock.store.core;

import io.lettuce.core.codec.RedisCodec;
import com.yahoo.sherlock.store.StoreParams;

/**
 * This class manages a standalone redis client instance to produce basic connections.
 */
public class ConnectionProducerImpl implements ConnectionProducer {

    /**
     * @param params Store parameters to initialize the client
     */
    protected ConnectionProducerImpl(StoreParams params) {
        Client.get().initializeRedisClient(params);
    }

    @Override
    public <K> RedisConnection<K> produce(RedisCodec<K, K> codec) {
        return new RedisConnectionImpl<>(Client.get().getRedisClient().connect(codec));
    }
}
