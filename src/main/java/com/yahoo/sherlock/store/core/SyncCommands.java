package com.yahoo.sherlock.store.core;

import com.lambdaworks.redis.Range;
import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ScriptOutputType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to wrap either a {@code RedisCommands} or {@code RedisClusterCommands} instance.
 *
 * @param <K> Redis primary type
 */
public interface SyncCommands<K> extends AutoCloseable {

    /**
     * @param key long key to increment
     * @return value before increment
     * @see com.lambdaworks.redis.api.sync.RedisCommands#incr(Object)
     */
    Long incr(K key);

    /**
     * @param key set key
     * @return members in the set
     * @see com.lambdaworks.redis.api.sync.RedisCommands#smembers(Object)
     */
    Set<K> smembers(K key);

    /**
     * @param key hash key
     * @return hash map value
     * @see com.lambdaworks.redis.api.sync.RedisCommands#hgetall(Object)
     */
    Map<K, K> hgetall(K key);

    /**
     * @param key   sorted set key
     * @param score value score
     * @param value value to add
     * @return number of added values
     * @see com.lambdaworks.redis.api.sync.RedisCommands#zadd(Object, double, Object)
     */
    Long zadd(K key, double score, K value);

    /**
     * @return always 'OK'
     * @see com.lambdaworks.redis.api.sync.RedisCommands#multi()
     */
    String multi();

    /**
     * @return list of Object responses from transaction commands
     * @see com.lambdaworks.redis.api.sync.RedisCommands#exec()
     */
    List<Object> exec();

    /**
     * @param key    sorted set key
     * @param values values to remove
     * @return number of removed elements
     * @see com.lambdaworks.redis.api.sync.RedisCommands#zrem(Object, Object[])
     */
    Long zrem(K key, K... values);

    /**
     * @param keys keys to delete
     * @return number of deleted keys
     * @see com.lambdaworks.redis.api.sync.RedisCommands#del(Object[])
     */
    Long del(K... keys);

    /**
     * @param key   sorted set key
     * @param start value start index
     * @param end   value end index
     * @return list of values
     * @see com.lambdaworks.redis.api.sync.RedisCommands#zrangeWithScores(Object, long, long)
     */
    List<ScoredValue<K>> zrangeWithScores(K key, long start, long end);

    /**
     * @param key   sorted set key
     * @param range value score range
     * @param <N>   range type
     * @return number of elements in the range
     * @see com.lambdaworks.redis.api.sync.RedisCommands#zcount(Object, Range)
     */
    <N extends Number> Long zcount(K key, Range<N> range);

    /**
     * @param script String of the script to execute
     * @param type   script return value type token
     * @param keys   keys operated on by the script
     * @param values script arguments
     * @param <T>    script return type
     * @return script results
     * @see com.lambdaworks.redis.api.sync.RedisCommands#eval(String, ScriptOutputType, Object[], Object[])
     */
    <T> T eval(String script, ScriptOutputType type, K[] keys, K... values);

    /**
     * @param key key name
     * @param seconds time in seconds
     * @return true if expiration is set else false
     */
    Boolean expire(K key, long seconds);

    @Override
    void close();
}
