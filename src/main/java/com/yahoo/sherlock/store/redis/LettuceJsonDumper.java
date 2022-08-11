/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.JsonDumper;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.utils.TimeUtils;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

    /* Gson object */
    private static Gson gson;

    /* expiry time for reports in redis */
    private final long expirationTime = Constants.SECONDS_IN_DAY * 100;

    private static final String[] INDEX_NAMES = {
        DatabaseConstants.INDEX_REPORT_JOB_ID,
        DatabaseConstants.INDEX_TIMESTAMP,
        DatabaseConstants.INDEX_DELETED_ID,
        DatabaseConstants.INDEX_CLUSTER_ID,
        DatabaseConstants.INDEX_QUERY_ID,
        DatabaseConstants.INDEX_JOB_CLUSTER_ID,
        DatabaseConstants.INDEX_JOB_ID,
        DatabaseConstants.INDEX_JOB_STATUS,
        DatabaseConstants.INDEX_FREQUENCY,
        DatabaseConstants.INDEX_EMAILID_REPORT,
        DatabaseConstants.INDEX_EMAIL_ID,
        DatabaseConstants.INDEX_EMAILID_TRIGGER,
        DatabaseConstants.INDEX_EMAILID_JOBID
    };

    /**
     * @param params store params
     */
    public LettuceJsonDumper(StoreParams params) {
        super(params);
        gson = new Gson();
    }

    @Override
    public List<ImmutablePair<String, String>> getQueuedJobs() throws IOException {
        try (
            RedisConnection<String> conn = connect();
            RedisConnection<byte[]> binary = binary()
        ) {
            List<RedisFuture> futures = new LinkedList<>();
            AsyncCommands<String> cmd = conn.async();
            AsyncCommands<byte[]> bin = binary.async();
            cmd.setAutoFlushCommands(false);
            bin.setAutoFlushCommands(false);
            StoreParams params = Store.getParamsFor(Store.AccessorType.ANOMALY_REPORT);
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
            futures.add(jobQueue);
            futures.add(pendingQueue);
            cmd.flushCommands();
            awaitRaw(futures);
            futures.clear();
            Gson gson = new Gson();
            List<ImmutablePair<String, String>> result = new ArrayList<>();
            JsonElement queueEl = gson.toJsonTree(jobQueue.get(), new TypeToken<List<ScoredValue<String>>>() { }.getType());
            JsonElement pendingEl = gson.toJsonTree(pendingQueue.get(), new TypeToken<List<ScoredValue<String>>>() { }.getType());
            JsonArray queueArray = queueEl.getAsJsonArray();
            JsonArray pendingQueueArray = pendingEl.getAsJsonArray();
            for (int i = 0; i < queueArray.size(); i++) {
                result.add(extractJobDetails(queueArray.get(i)));
            }
            for (int i = 0; i < pendingQueueArray.size(); i++) {
                result.add(extractJobDetails(pendingQueueArray.get(i)));
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while retrieving Redis database!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Utility method to extract job details from the queue.
     * @param jsonElement input json element
     * @return a pair of (jobid, nextRunTime)
     */
    private ImmutablePair<String, String> extractJobDetails(JsonElement jsonElement) {
        Integer jobId = jsonElement.getAsJsonObject().getAsJsonPrimitive("value").getAsInt();
        String time = TimeUtils.getFormattedTimeMinutes(jsonElement.getAsJsonObject().getAsJsonPrimitive("score").getAsInt());
        return new ImmutablePair<>(jobId.toString(), time);
    }

    @Override
    public JsonObject getRawData() throws IOException {
        try (
                RedisConnection<String> conn = connect();
                RedisConnection<byte[]> binary = binary()
        ) {
            log.info("Starting to fetch all redis keys...");
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
            log.info("Fetched queued jobs...");
            for (String indexKey : INDEX_NAMES) {
                indexKeys.put(params.get(indexKey), cmd.keys(index(params.get(indexKey), "*")));
            }
            for (Store.AccessorType type : Store.AccessorType.values()) {
                params = Store.getParamsFor(type);
                globals.put(params.get(DatabaseConstants.ID_NAME), cmd.get(params.get(DatabaseConstants.ID_NAME)));
                hashKeys.put(params.get(DatabaseConstants.DB_NAME), cmd.keys(index(params.get(DatabaseConstants.DB_NAME), "*")));
            }
            log.info("Fetched all keys...");
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
            log.info("Fetched all objects...");
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
            log.info("sending back the json with {} keys", result.size());
            return result;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while retrieving Redis database!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Helper to instantiate jobs.
     * @param job job json object
     * @return instantiated JobMetadata object
     */
    private JobMetadata instantiateJobs(JsonObject job) {
        job.addProperty("effectiveRunTime", (String) null);
        job.addProperty("effectiveQueryTime", (String) null);
        // for backward compatibility
        if (!job.has("timeseriesRange") || ("").equals(job.getAsJsonPrimitive("timeseriesRange").getAsString())) {
            switch (job.getAsJsonPrimitive("granularity").getAsString().toUpperCase()) {
                case Constants.MINUTE : job.addProperty("timeseriesRange", CLISettings.INTERVAL_MINUTES);
                case Constants.HOUR : job.addProperty("timeseriesRange", CLISettings.INTERVAL_MINUTES);
                case Constants.DAY : job.addProperty("timeseriesRange", CLISettings.INTERVAL_MINUTES);
                case Constants.WEEK : job.addProperty("timeseriesRange", CLISettings.INTERVAL_MINUTES);
                case Constants.MONTH : job.addProperty("timeseriesRange", CLISettings.INTERVAL_MINUTES);
            }
        }
        JobMetadata jobMetadata = gson.fromJson(job, JobMetadata.class);
        jobMetadata.setJobStatus(JobStatus.CREATED.getValue());
        return jobMetadata;
    }

    @Override
    public void writeRawData(JsonObject json) throws IOException {
        Set<String> jsonKeys = json.keySet();
        Map<String, Map<String, String>> modelObjectKeys = new HashMap<>();
        Map<String, String[]> indexKeys = new HashMap<>();
        Map<String, List<ScoredValue<byte[]>>> anomalyTimestampKeys = new HashMap<>();
        Map<String, String> idKeys = new HashMap<>();
        Mapper<String> jobObjectMapper = new HashMapper();
        Mapper<String> emailObjectMapper = new HashMapper();
        Mapper<String> reportObjectMapper = new HashMapper();
        Mapper<String> druidClusterObjectMapper = new HashMapper();

        for (String key : jsonKeys) {
            if (json.get(key).isJsonObject()) {
                if (key.contains(DatabaseConstants.JOBS) && !key.contains(DatabaseConstants.DELETED_JOBS)) {
                    modelObjectKeys.put(key, jobObjectMapper.map(instantiateJobs(json.getAsJsonObject(key))));
                } else if (key.contains(DatabaseConstants.REPORTS)) {
                    modelObjectKeys.put(key, reportObjectMapper.map(gson.fromJson(json.getAsJsonObject(key), AnomalyReport.class)));
                } else if (key.contains(DatabaseConstants.EMAILS)) {
                    modelObjectKeys.put(key, emailObjectMapper.map(gson.fromJson(json.getAsJsonObject(key), EmailMetaData.class)));
                } else if (key.contains(DatabaseConstants.DRUID_CLUSTERS)) {
                    modelObjectKeys.put(key, druidClusterObjectMapper.map(gson.fromJson(json.getAsJsonObject(key), DruidCluster.class)));
                }
            } else if (json.get(key).isJsonArray()) {
                if (key.contains(DatabaseConstants.ANOMALY_TIMESTAMP)) {
                    anomalyTimestampKeys.put(key, gson.fromJson(json.getAsJsonArray(key), new TypeToken<List<ScoredValue<byte[]>>>() { }.getType()));
                } else if (key.contains(DatabaseConstants.INDEX) && !key.contains(DatabaseConstants.DELETED)) {
                    indexKeys.put(key, gson.fromJson(json.getAsJsonArray(key), String[].class));
                }
            } else if (key.equals(DatabaseConstants.CLUSTER_ID) || key.equals(DatabaseConstants.JOB_ID)) {
                idKeys.put(key, json.getAsJsonPrimitive(key).getAsString());
            } else {
                log.error("Key is not a Json object, Json array or Json primitive: key = {}", key);
            }
        }
        if (modelObjectKeys.size() > 0) {
            writeObjectsToRedis(modelObjectKeys);
        } else {
            log.info("Found zero objects in json dump!");
        }
        if (anomalyTimestampKeys.size() > 0) {
            writeAnomalyTimestampsToRedis(anomalyTimestampKeys);
        } else {
            log.info("Found zero anomaly timestamps in json dump!");
        }
        if (indexKeys.size() > 0) {
            writeIndexesToRedis(indexKeys);
        } else {
            log.info("Found zero index keys in json dump!");
        }
        if (idKeys.size() > 0) {
            writeIdsToRedis(idKeys);
        } else {
            log.info("Found zero Ids in json dump!");
        }
        log.info("Json dump is populated into redis.");
    }

    @Override
    public void clearIndexes(String index, String id) {
        String indexName = index + Constants.COLON_DELIMITER + id;
        RedisConnection<String> conn = connect();
        Long redisResponse = conn.sync().del(indexName);
        log.info("Deleted index named {} with redis response {}", indexName, redisResponse);
    }

    /**
     * Method to write objects ({@link JobMetadata}, {@link EmailMetaData etc.}) to redis.
     * @param objects map : key - object key, value - object fields as a map of strings
     */
    public void writeObjectsToRedis(Map<String, Map<String, String>> objects) {
        RedisConnection<String> conn = connect();
        List<RedisFuture> futures = new LinkedList<>();
        AsyncCommands<String> cmd = conn.async();
        cmd.setAutoFlushCommands(false);
        log.info("Adding {} objects to redis", objects.size());
        for (Map.Entry<String, Map<String, String>> object : objects.entrySet()) {
            futures.add(cmd.hmset(object.getKey(), object.getValue()));
            if (object.getKey().contains(DatabaseConstants.REPORTS)) {
                futures.add(cmd.expire(object.getKey(), expirationTime));
            }
        }
        cmd.flushCommands();
        awaitRaw(futures);
        log.info("Added all objects to redis");
    }

    /**
     * Method to write anomaly timestamps from ({@link AnomalyReport}) to redis.
     * @param anomalyTimestamps map : key - redis key, value - list of timestamps as {@link ScoredValue}
     */
    public void writeAnomalyTimestampsToRedis(Map<String, List<ScoredValue<byte[]>>> anomalyTimestamps) {
        RedisConnection<byte[]> conn = binary();
        List<RedisFuture> futures = new LinkedList<>();
        AsyncCommands<byte[]> cmd = conn.async();
        cmd.setAutoFlushCommands(false);
        log.info("Adding {} anomaly timestamps to redis", anomalyTimestamps.size());
        for (Map.Entry<String, List<ScoredValue<byte[]>>> anomalyTimestamp : anomalyTimestamps.entrySet()) {
            futures.add(cmd.zadd(encode(anomalyTimestamp.getKey()), anomalyTimestamp.getValue().toArray(new ScoredValue[anomalyTimestamp.getValue().size()])));
            futures.add(cmd.expire(encode(anomalyTimestamp.getKey()), expirationTime));
        }
        cmd.flushCommands();
        awaitRaw(futures);
        log.info("Added all anomaly timestamps to redis");
    }

    /**
     * Method to write inices to redis.
     * @param indices map : key - index key, value - string array of index values
     */
    public void writeIndexesToRedis(Map<String, String[]> indices) {
        RedisConnection<String> conn = connect();
        List<RedisFuture<Long>> futures = new LinkedList<>();
        AsyncCommands<String> cmd = conn.async();
        cmd.setAutoFlushCommands(false);
        log.info("Adding {} indices to redis", indices.size());
        for (Map.Entry<String, String[]> index : indices.entrySet()) {
            futures.add(cmd.sadd(index.getKey(), index.getValue()));
        }
        cmd.flushCommands();
        await(futures);
        log.info("Added all indices to redis");
    }

    /**
     * Method to write ids (like jobId, clusterId) to redis.
     * @param ids map : key - id name, value - id value
     */
    public void writeIdsToRedis(Map<String, String> ids) {
        RedisConnection<String> conn = connect();
        List<RedisFuture<String>> futures = new LinkedList<>();
        AsyncCommands<String> cmd = conn.async();
        cmd.setAutoFlushCommands(false);
        log.info("Adding {} Ids to redis", ids.size());
        for (Map.Entry<String, String> id : ids.entrySet()) {
            futures.add(cmd.set(id.getKey(), id.getValue()));
        }
        cmd.flushCommands();
        await(futures);
        log.info("Added all Ids to redis");
    }
}
