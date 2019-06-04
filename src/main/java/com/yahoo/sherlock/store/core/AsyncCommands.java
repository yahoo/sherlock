package com.yahoo.sherlock.store.core;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to wrap either a {@code io.lettuce.core.api.async.RedisAsyncCommands} or {@code RedisClusterAsyncComamands}
 * instance to provide code reusability.
 *
 * @param <K> Redis primary type
 */
public interface AsyncCommands<K> extends AutoCloseable {

    /**
     * @param flush whether to autoflush
     * @see io.lettuce.core.api.async.RedisAsyncCommands#setAutoFlushCommands(boolean)
     */
    void setAutoFlushCommands(boolean flush);

    /**
     * @see io.lettuce.core.api.async.RedisAsyncCommands#flushCommands()
     */
    void flushCommands();

    /**
     * @param pattern key matching pattern
     * @return list of matching keys
     * @see io.lettuce.core.api.async.RedisAsyncCommands#keys(Object)
     */
    RedisFuture<List<K>> keys(K pattern);

    /**
     * @param key key to get
     * @param value value of the key
     * @return Ok if set the key
     * @see io.lettuce.core.api.async.RedisAsyncCommands#set(Object, Object)
     */
    RedisFuture<String> set(K key, K value);

    /**
     * @param key key to get
     * @return value of the key
     * @see io.lettuce.core.api.async.RedisAsyncCommands#get(Object)
     */
    RedisFuture<K> get(K key);

    /**
     * @param key long key value to increment
     * @return value before increment
     * @see io.lettuce.core.api.async.RedisAsyncCommands#incr(Object)
     */
    RedisFuture<Long> incr(K key);

    /**
     * @param key    set key
     * @param values values to add
     * @return number of added values
     * @see io.lettuce.core.api.async.RedisAsyncCommands#sadd(Object, Object[])
     */
    RedisFuture<Long> sadd(K key, K... values);

    /**
     * @param key    set key
     * @param values values to remove
     * @return number of removed values
     * @see io.lettuce.core.api.async.RedisAsyncCommands#srem(Object, Object[])
     */
    RedisFuture<Long> srem(K key, K... values);

    /**
     * @param keys keys to delete
     * @return number of deleted keys
     * @see io.lettuce.core.api.async.RedisAsyncCommands#del(Object[])
     */
    RedisFuture<Long> del(K... keys);

    /**
     * @param key set key
     * @return elements of the set
     * @see io.lettuce.core.api.async.RedisAsyncCommands#smembers(Object)
     */
    RedisFuture<Set<K>> smembers(K key);

    /**
     * @param key hash key
     * @param h   hash map to use
     * @return always 'OK'
     * @see io.lettuce.core.api.async.RedisAsyncCommands#hmset(Object, Map)
     */
    RedisFuture<String> hmset(K key, Map<K, K> h);

    /**
     * @param key hash key to get
     * @return hash map at the location
     * @see io.lettuce.core.api.async.RedisAsyncCommands#hgetall(Object)
     */
    RedisFuture<Map<K, K>> hgetall(K key);

    /**
     * @param key    sorted set key
     * @param values scored values to add
     * @return number of added elements
     * @see io.lettuce.core.api.async.RedisAsyncCommands#zadd(Object, Object...)
     */
    RedisFuture<Long> zadd(K key, ScoredValue<K>... values);

    /**
     * @param key   sorted set key
     * @param start start index
     * @param end   end index
     * @return list of values in those indices
     * @see io.lettuce.core.api.async.RedisAsyncCommands#zrangeWithScores(Object, long, long)
     */
    RedisFuture<List<ScoredValue<K>>> zrangeWithScores(K key, long start, long end);

    /**
     * @param key key name
     * @param seconds time in seconds
     * @return true if expiration is set else false
     */
    RedisFuture<Boolean> expire(K key, long seconds);

    /**
     * Command to dump snapshot of redis data.
     * @return OK string
     */
    RedisFuture<String> bgsave();

}
