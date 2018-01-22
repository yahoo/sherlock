package com.yahoo.sherlock.store.redis;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.ScoredValue;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.JsonDumper;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static com.yahoo.sherlock.store.redis.Mapper.encode;

/**
 * Json dumper for a clustered redis instance.
 */
@Slf4j
public class LettuceJsonDumper
        extends AbstractLettuceAccessor
    implements JsonDumper {

    /**
     * @param params store params
     */
    public LettuceJsonDumper(StoreParams params) {
        super(params);
    }

    @Override
    public JsonObject getRawData() throws IOException {
        try (
                RedisConnection<String> conn = connect();
                RedisConnection<byte[]> binary = binary()
        ) {
            List<RedisFuture> futures = new LinkedList<>();
            AsyncCommands<String> cmd = conn.async();
            AsyncCommands<byte[]> bin = binary.async();
            Map<String, RedisFuture<String>> globals = new TreeMap<>();
            Map<String, RedisFuture<Set<String>>> indices = new TreeMap<>();
            Map<String, RedisFuture<Map<String, String>>> hashes = new TreeMap<>();
            Map<String, RedisFuture<List<ScoredValue<byte[]>>>> binaries = new TreeMap<>();
            Map<String, RedisFuture<List<String>>> hashKeys = new TreeMap<>();
            Map<String, RedisFuture<List<String>>> indexKeys = new TreeMap<>();
            List<String> binaryKeys = new LinkedList<>();
            StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
            cmd.setAutoFlushCommands(false);
            bin.setAutoFlushCommands(false);
            String queueName = DatabaseConstants.QUEUE_JOB_SCHEDULE;
            String pendingQueueName = queueName + "Pending";
            queueName = String.format("{queue}.%s", queueName);
            pendingQueueName = String.format("{queue}.%s", pendingQueueName);
            RedisFuture<List<ScoredValue<String>>> jobQueue = cmd.zrangeWithScores(
                queueName, 0, -1
            );
            RedisFuture<List<ScoredValue<String>>> pendingQueue = cmd.zrangeWithScores(
                pendingQueueName + "Pending", 0, -1
            );
            for (String indexKey : INDEX_NAMES) {
                indexKeys.put(params.get(indexKey), cmd.keys(index(params.get(indexKey), "*")));
            }
            for (Store.AccessorType type : Store.AccessorType.values()) {
                params = Store.getParamsFor(type);
                globals.put(params.get(DatabaseConstants.ID_NAME), cmd.get(params.get(DatabaseConstants.ID_NAME)));
                hashKeys.put(params.get(DatabaseConstants.DB_NAME), cmd.keys(index(params.get(DatabaseConstants.DB_NAME), "*")));
            }
            futures.add(jobQueue);
            futures.add(pendingQueue);
            futures.addAll(indexKeys.values());
            futures.addAll(globals.values());
            futures.addAll(hashKeys.values());
            cmd.flushCommands();
            awaitRaw(futures);
            futures.clear();
            for (RedisFuture<List<String>> indexKeyList : indexKeys.values()) {
                for (String indexKey : indexKeyList.get()) {
                    indices.put(indexKey, cmd.smembers(indexKey));
                }
            }
            for (RedisFuture<List<String>> hashKeyList : hashKeys.values()) {
                for (String hashKey : hashKeyList.get()) {
                    if (hashKey.contains(DatabaseConstants.ANOMALY_TIMESTAMP)) {
                        binaryKeys.add(hashKey);
                    } else {
                        hashes.put(hashKey, cmd.hgetall(hashKey));
                    }
                }
            }
            for (String binaryKey : binaryKeys) {
                byte[] key = encode(binaryKey);
                binaries.put(binaryKey, bin.zrangeWithScores(key, 0, -1));
            }
            cmd.flushCommands();
            bin.flushCommands();
            futures.addAll(binaries.values());
            futures.addAll(hashes.values());
            futures.addAll(indices.values());
            awaitRaw(futures);
            futures.clear();
            Gson gson = new Gson();
            JsonObject result = new JsonObject();
            JsonElement queueEl = gson.toJsonTree(jobQueue.get(), new TypeToken<List<ScoredValue<String>>>() { }.getType());
            JsonElement pendingEl = gson.toJsonTree(pendingQueue.get(), new TypeToken<List<ScoredValue<String>>>() { }.getType());
            result.add(params.get(DatabaseConstants.QUEUE_JOB_SCHEDULE), queueEl);
            result.add(params.get(DatabaseConstants.QUEUE_JOB_SCHEDULE) + "Pending", pendingEl);
            for (Map.Entry<String, RedisFuture<String>> global : globals.entrySet()) {
                result.addProperty(global.getKey(), global.getValue().get());
            }
            for (Map.Entry<String, RedisFuture<Set<String>>> index : indices.entrySet()) {
                result.add(index.getKey(), gson.toJsonTree(index.getValue().get(), new TypeToken<Set<String>>() { }.getType()));
            }
            for (Map.Entry<String, RedisFuture<Map<String, String>>> hash : hashes.entrySet()) {
                result.add(hash.getKey(), gson.toJsonTree(hash.getValue().get(), new TypeToken<Map<String, String>>() { }.getType()));
            }
            for (Map.Entry<String, RedisFuture<List<ScoredValue<byte[]>>>> binEl : binaries.entrySet()) {
                result.add(binEl.getKey(), gson.toJsonTree(binEl.getValue().get(), new TypeToken<List<ScoredValue<byte[]>>>() { }.getType()));
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while retrieving Redis database!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void writeRawData(JsonObject json) throws IOException {
        // TODO: left unimplemented since this isn't currently used anywhere
        throw new NotImplementedException();
    }
}
