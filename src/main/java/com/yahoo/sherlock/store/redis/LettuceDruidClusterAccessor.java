/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import io.lettuce.core.RedisFuture;
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Druid cluster accessor implemented for redis clusters.
 */
@Slf4j
public class LettuceDruidClusterAccessor
        extends AbstractLettuceAccessor
    implements DruidClusterAccessor {

    private final String clusterIdName;

    /**
     * @param params store parameters
     */
    public LettuceDruidClusterAccessor(StoreParams params) {
        super(params);
        this.clusterIdName = params.get(DatabaseConstants.INDEX_CLUSTER_ID);
    }

    @Override
    public DruidCluster getDruidCluster(String clusterId) throws IOException, ClusterNotFoundException {
        log.info("Getting Druid cluster [{}]", clusterId);
        try (RedisConnection<String> conn = connect()) {
            Map<String, String> clusterMap = conn.sync().hgetall(key(clusterId));
            if (clusterMap.isEmpty()) {
                throw new ClusterNotFoundException(clusterId);
            }
            return unmap(DruidCluster.class, clusterMap);
        }
    }

    @Override
    public void putDruidCluster(DruidCluster cluster) throws IOException {
        log.info("Putting Druid cluster [{}]", cluster.getClusterId());
        try (RedisConnection<String> conn = connect()) {
            if (cluster.getClusterId() == null) {
                cluster.setClusterId(newId());
            }
            Map<String, String> clusterMap = map(cluster);
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<String> hmsetRes = cmd.hmset(key(cluster.getClusterId()), clusterMap);
            RedisFuture<Long> saddRes = cmd.sadd(index(clusterIdName, "all"),
                    cluster.getClusterId().toString());
            cmd.flushCommands();
            await(hmsetRes, saddRes);
        }
    }

    @Override
    public void deleteDruidCluster(String clusterId) throws IOException, ClusterNotFoundException {
        log.info("Deleting Druid cluster [{}]", clusterId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Long> sremRes = cmd.srem(index(clusterIdName, "all"), clusterId);
            RedisFuture<Long> delRes = cmd.del(key(clusterId));
            cmd.flushCommands();
            await(sremRes, delRes);
            if (delRes.get() == 0) {
                throw new ClusterNotFoundException(clusterId);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while deleting Druid cluster!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<DruidCluster> getDruidClusterList() throws IOException {
        log.info("Getting Druid clusters list");
        try (RedisConnection<String> conn = connect()) {
            Set<String> clusterIds = conn.sync().smembers(index(clusterIdName, "all"));
            Set<RedisFuture<Map<String, String>>> clusterFutures = new HashSet<>((int) (1.5 * clusterIds.size()));
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            for (String clusterId : clusterIds) {
                clusterFutures.add(cmd.hgetall(key(clusterId)));
            }
            cmd.flushCommands();
            await(clusterFutures);
            List<DruidCluster> clusters = new ArrayList<>(clusterFutures.size());
            for (RedisFuture<Map<String, String>> clusterFuture : clusterFutures) {
                clusters.add(unmap(DruidCluster.class, clusterFuture.get()));
            }
            log.info("Successfully retrieved [{}] Druid clusters", clusters.size());
            return clusters;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting Druid cluster list");
            throw new IOException(e.getMessage(), e);
        }
    }

}
