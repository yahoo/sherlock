/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.core;

import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;

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
     * @param key key to get
     * @param value value of the key
     * @return Ok if set the key
     * @see io.lettuce.core.api.sync.RedisCommands#set(Object, Object)
     */
    String set(K key, K value);

    /**
     * @param key key to get
     * @return value of the key
     * @see io.lettuce.core.api.sync.RedisCommands#get(Object)
     */
    K get(K key);

    /**
     * @param key long key to increment
     * @return value before increment
     * @see io.lettuce.core.api.sync.RedisCommands#incr(Object)
     */
    Long incr(K key);

    /**
     * @param key set key
     * @return members in the set
     * @see io.lettuce.core.api.sync.RedisCommands#smembers(Object)
     */
    Set<K> smembers(K key);

    /**
     * @param key    set key
     * @param values values to remove
     * @return number of removed values
     * @see io.lettuce.core.api.sync.RedisCommands#srem(Object, Object[])
     */
    Long srem(K key, K... values);

    /**
     * @param key hash key
     * @return hash map value
     * @see io.lettuce.core.api.sync.RedisCommands#hgetall(Object)
     */
    Map<K, K> hgetall(K key);

    /**
     * @param key   sorted set key
     * @param score value score
     * @param value value to add
     * @return number of added values
     * @see io.lettuce.core.api.sync.RedisCommands#zadd(Object, double, Object)
     */
    Long zadd(K key, double score, K value);

    /**
     * @param key    sorted set key
     * @param values values to remove
     * @return number of removed elements
     * @see io.lettuce.core.api.sync.RedisCommands#zrem(Object, Object[])
     */
    Long zrem(K key, K... values);

    /**
     * @param keys keys to delete
     * @return number of deleted keys
     * @see io.lettuce.core.api.sync.RedisCommands#del(Object[])
     */
    Long del(K... keys);

    /**
     * @param key   sorted set key
     * @param start value start index
     * @param end   value end index
     * @return list of values
     * @see io.lettuce.core.api.sync.RedisCommands#zrangeWithScores(Object, long, long)
     */
    List<ScoredValue<K>> zrangeWithScores(K key, long start, long end);

    /**
     * @param key   sorted set key
     * @param range value score range
     * @param <N>   range type
     * @return number of elements in the range
     * @see io.lettuce.core.api.sync.RedisCommands#zcount(Object, Range)
     */
    <N extends Number> Long zcount(K key, Range<N> range);

    /**
     * @param script String of the script to execute
     * @param type   script return value type token
     * @param keys   keys operated on by the script
     * @param values script arguments
     * @param <T>    script return type
     * @return script results
     * @see io.lettuce.core.api.sync.RedisCommands#eval(String, ScriptOutputType, Object[], Object[])
     */
    <T> T eval(String script, ScriptOutputType type, K[] keys, K... values);

    /**
     * @param key key name
     * @param seconds time in seconds
     * @return true if expiration is set else false
     */
    Boolean expire(K key, long seconds);

    /**
     * Command to dump snapshot of redis data.
     * @return OK string
     */
    String bgsave();

}
