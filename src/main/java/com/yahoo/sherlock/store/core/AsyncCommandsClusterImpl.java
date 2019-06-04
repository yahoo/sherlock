package com.yahoo.sherlock.store.core;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements {@code AsyncCommands} to wrap a {@code RedisClusterAsyncCommands} instance.
 *
 * @param <K> Redis primary type
 */
public class AsyncCommandsClusterImpl<K> implements AsyncCommands<K> {

    private final RedisClusterAsyncCommands<K, K> commands;

    /**
     * @param commands cluster commands instance to wrap
     */
    protected AsyncCommandsClusterImpl(RedisClusterAsyncCommands<K, K> commands) {
        this.commands = commands;
    }

    @Override
    public void setAutoFlushCommands(boolean flush) {
        commands.setAutoFlushCommands(flush);
    }

    @Override
    public void flushCommands() {
        commands.flushCommands();
    }

    @Override
    public RedisFuture<List<K>> keys(K pattern) {
        return commands.keys(pattern);
    }

    @Override
    public RedisFuture<String> set(K key, K value) {
        return commands.set(key, value);
    }

    @Override
    public RedisFuture<K> get(K key) {
        return commands.get(key);
    }

    @Override
    public RedisFuture<Long> incr(K key) {
        return commands.incr(key);
    }

    @Override
    public RedisFuture<Long> sadd(K key, K... values) {
        return commands.sadd(key, values);
    }

    @Override
    public RedisFuture<Long> srem(K key, K... values) {
        return commands.srem(key, values);
    }

    @Override
    public RedisFuture<Long> del(K... keys) {
        return commands.del(keys);
    }

    @Override
    public RedisFuture<Set<K>> smembers(K key) {
        return commands.smembers(key);
    }

    @Override
    public RedisFuture<String> hmset(K key, Map<K, K> h) {
        return commands.hmset(key, h);
    }

    @Override
    public RedisFuture<Map<K, K>> hgetall(K key) {
        return commands.hgetall(key);
    }

    @Override
    public RedisFuture<Long> zadd(K key, ScoredValue<K>... values) {
        return commands.zadd(key, values);
    }

    @Override
    public RedisFuture<List<ScoredValue<K>>> zrangeWithScores(K key, long start, long end) {
        return commands.zrangeWithScores(key, start, end);
    }

    @Override
    public RedisFuture<Boolean> expire(K key, long seconds) {
        return commands.expire(key, seconds);
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public RedisFuture<String> bgsave() {
        return commands.bgsave();
    }
}
