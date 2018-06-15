package com.yahoo.sherlock.store.redis;

import com.lambdaworks.redis.LettuceFutures;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisFuture;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.BaseAccessor;
import com.yahoo.sherlock.store.core.RedisConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Abstract accessor used for all Lettuce accessors.
 * Contains common methods needed by all accessors.
 */
@Slf4j
public class AbstractLettuceAccessor extends BaseAccessor {

    private final String keyName;
    private final String idName;
    private final Mapper<String> mapper;
    private final int timeoutMillis;

    /**
     * Constructor that pulls the basic key prefix and ID
     * name, and the timeout used for waiting on async commands.
     *
     * @param params store parameters
     */
    public AbstractLettuceAccessor(StoreParams params) {
        super(params, params.get(DatabaseConstants.REDIS_CLUSTERED) != null);
        this.keyName = params.get(DatabaseConstants.DB_NAME);
        this.idName = params.get(DatabaseConstants.ID_NAME);
        this.timeoutMillis = Integer.parseInt(params.get(DatabaseConstants.REDIS_TIMEOUT));
        mapper = new HashMapper();
    }

    /**
     * Await an array of futures.
     *
     * @param futures redis futures to await
     */
    protected void await(RedisFuture... futures) {
        LettuceFutures.awaitAll(timeoutMillis, TimeUnit.MILLISECONDS, futures);
    }

    /**
     * Await a collection of typed futures.
     *
     * @param futures collection of futures
     * @param <FutureType> future type
     */
    protected <FutureType> void await(Collection<RedisFuture<FutureType>> futures) {
        await(futures.toArray(new RedisFuture[futures.size()]));
    }

    /**
     * Await a collection of raw type futures.
     *
     * @param futures collection of raw type futures
     */
    protected void awaitRaw(Collection<RedisFuture> futures) {
        await(futures.toArray(new RedisFuture[futures.size()]));
    }

    /**
     * Await a collection of arrays of redis futures. This
     * method will join the futures in an array.
     *
     * @param futuresArr collection of arrays of futures
     */
    protected void awaitCollection(Collection<RedisFuture[]> futuresArr) {
        int size = 0;
        for (RedisFuture[] arr : futuresArr) {
            size += arr.length;
        }
        RedisFuture[] array = new RedisFuture[size];
        int i = 0;
        for (RedisFuture[] arr : futuresArr) {
            System.arraycopy(arr, 0, array, i, arr.length);
            i += arr.length;
        }
        await(array);
    }

    /**
     * Produce a joined key separated by colons.
     *
     * @param qualifiers key qualifiers, appended to the key name
     * @return a full key
     */
    protected String key(Object... qualifiers) {
        StringJoiner joiner = new StringJoiner(":");
        joiner.add(keyName);
        for (Object qualifier : qualifiers) {
            joiner.add(qualifier == null ? null : qualifier.toString());
        }
        return joiner.toString();
    }

    /**
     * Produce a joined index separated by colons.
     *
     * @param qualifiers index qualifiers
     * @return a full index
     */
    protected static String index(Object... qualifiers) {
        StringJoiner joiner = new StringJoiner(":");
        for (Object qualifier : qualifiers) {
            joiner.add(qualifier == null ? null : qualifier.toString());
        }
        return joiner.toString();
    }

    /**
     * @return a new incremented ID
     * @throws IOException if an error generating the ID occurs
     */
    protected Integer newId() throws IOException {
        log.info("Generating a new ID");
        try (RedisConnection<String> conn = connect()) {
            return conn.sync().incr(idName).intValue();
        } catch (RedisException e) {
            log.error("Error while generating new ID!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * @param n the number of IDs to generate
     * @return an array of string IDs
     * @throws IOException if an error generating the ID occurs
     */
    protected Integer[] newIds(int n) throws IOException {
        log.info("Generating [{}] new IDs", n);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            List<RedisFuture<Long>> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ids.add(cmd.incr(idName));
            }
            cmd.flushCommands();
            await(ids);
            Integer[] idsArr = new Integer[n];
            for (int i = 0; i < n; i++) {
                idsArr[i] = ids.get(i).get().intValue();
            }
            return idsArr;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while generating bulk IDs!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * @param t object to map
     * @param <T> object type
     * @return string map
     */
    protected <T> Map<String, String> map(T t) {
        return mapper.map(t);
    }

    /**
     * @param cls class to unmap
     * @param map string map
     * @param <T> unmapped type
     * @return a new instance of the type
     */
    protected <T> T unmap(Class<T> cls, Map<String, String> map) {
        return mapper.unmap(cls, map);
    }

}
