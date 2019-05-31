package com.yahoo.sherlock.store.core;

import com.lambdaworks.redis.Range;
import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ScriptOutputType;
import com.lambdaworks.redis.api.sync.RedisCommands;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements {@code SyncCommands} to wrap a {@code RedisCommands} instance.
 *
 * @param <K> Redis primary type
 */
public class SyncCommandsImpl<K> implements SyncCommands<K> {

    private final RedisCommands<K, K> commands;

    /**
     * @param commands Redis commands to wrap
     */
    protected SyncCommandsImpl(RedisCommands<K, K> commands) {
        this.commands = commands;
    }

    @Override
    public String set(K key, K value) {
        return commands.set(key, value);
    }

    @Override
    public K get(K key) {
        return commands.get(key);
    }

    @Override
    public Long incr(K key) {
        return commands.incr(key);
    }

    @Override
    public Set<K> smembers(K key) {
        return commands.smembers(key);
    }

    @Override
    public Map<K, K> hgetall(K key) {
        return commands.hgetall(key);
    }

    @Override
    public Long zadd(K key, double score, K value) {
        return commands.zadd(key, score, value);
    }

    @Override
    public String multi() {
        return commands.multi();
    }

    @Override
    public List<Object> exec() {
        return commands.exec();
    }

    @Override
    public Long zrem(K key, K... values) {
        return commands.zrem(key, values);
    }

    @Override
    public Long del(K... keys) {
        return commands.del(keys);
    }

    @Override
    public List<ScoredValue<K>> zrangeWithScores(K key, long start, long end) {
        return commands.zrangeWithScores(key, start, end);
    }

    @Override
    public <N extends Number> Long zcount(K key, Range<N> range) {
        return commands.zcount(key, range);
    }

    @Override
    public <T> T eval(String script, ScriptOutputType type, K[] keys, K... values) {
        return commands.eval(script, type, keys, values);
    }

    @Override
    public Boolean expire(K key, long seconds) {
        return expire(key, seconds);
    }

    @Override
    public void close() {
        commands.close();
    }

    @Override
    public String bgsave() {
        return commands.bgsave();
    }
}
