package com.yahoo.sherlock.store.core;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import com.yahoo.sherlock.exception.StoreException;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.StoreParams;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * This single class manages a {@code RedisClient} and {@code RedisClusterClient}
 * instance, which are used to connect either to a standalone Redis instance or a cluster.
 */
public class Client {

    private static Client client = null;

    /**
     * @return the singleton instance of this class
     */
    public static Client get() {
        if (client == null) {
            client = new Client();
        }
        return client;
    }

    private RedisClient redisClient;
    private RedisClusterClient redisClusterClient;

    /**
     * @param hostname Redis hostname to validate
     */
    protected static void validateHostname(String hostname) {
        if (hostname == null || hostname.length() == 0) {
            throw new StoreException("Invalid Redis hostname: " + hostname);
        }
    }

    /**
     * @param portStr port number as a String value
     * @return Redis port number
     */
    protected static int getPortNumber(String portStr) {
        Scanner s = new Scanner(portStr);
        int port;
        if (!s.hasNextInt() || (port = s.nextInt()) < 0 || s.hasNext()) {
            throw new StoreException("Invalid Redis port: " + portStr);
        }
        return port;
    }

    /**
     * @param timeoutStr timeout as a String value
     * @return Redis timeout in milliseconds
     */
    protected static int getTimeout(String timeoutStr) {
        return getPortNumber(timeoutStr); // same logic for both
    }

    /**
     * @param sslStr SSL boolean value as a String
     * @return whether connection should use SSL
     */
    protected static boolean getSSL(String sslStr) {
        return null != sslStr && !"false".equals(sslStr.toLowerCase());
    }

    /**
     * @param hostname Redis hostname
     * @param port     Redis port
     * @param ssl      whether to use SSL
     * @param password Redis password
     * @param timeout  Redis timeout
     * @return a Redis URI used to create clients
     */
    protected static RedisURI produceURI(
            String hostname,
            String port,
            String ssl,
            String password,
            String timeout
    ) {
        validateHostname(hostname);
        RedisURI.Builder builder = RedisURI.Builder
                .redis(hostname, getPortNumber(port))
                .withSsl(getSSL(ssl))
                .withTimeout(getTimeout(timeout), TimeUnit.MILLISECONDS);
        if (null != password && !password.isEmpty()) {
            builder.withPassword(password);
        }
        return builder.build();
    }

    /**
     * @param params Store params from which to create a Redis URI
     * @return a Redis URI used to create clients
     */
    protected static RedisURI produceURI(StoreParams params) {
        return produceURI(
                params.get(DatabaseConstants.REDIS_HOSTNAME),
                params.get(DatabaseConstants.REDIS_PORT),
                params.get(DatabaseConstants.REDIS_SSL),
                params.get(DatabaseConstants.REDIS_PASSWORD),
                params.get(DatabaseConstants.REDIS_TIMEOUT)
        );
    }

    /**
     * Initialize the {@code RedisClient} if it has not already been.
     *
     * @param params Store params used to create a Redis URI
     */
    public void initializeRedisClient(StoreParams params) {
        if (redisClient != null) {
            return;
        }
        redisClient = RedisClient.create(produceURI(params));
    }

    /**
     * Initialize the {@code RedisClusterClient} if it has not already been.
     *
     * @param params Store params used to create a Redis URI
     */
    public void initializeRedisClusterClient(StoreParams params) {
        if (redisClusterClient != null) {
            return;
        }
        redisClusterClient = RedisClusterClient.create(produceURI(params));
        // Adaptive cluster topology refresh for redis cluster client
        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
            .enablePeriodicRefresh(true)
            .enableAllAdaptiveRefreshTriggers()
            .dynamicRefreshSources(true)
            .closeStaleConnections(true)
            .build();

        redisClusterClient.setOptions(ClusterClientOptions.builder()
                                          .validateClusterNodeMembership(true)
                                          .maxRedirects(5)
                                          .topologyRefreshOptions(topologyRefreshOptions)
                                          .build());
    }

    /**
     * @return the RedisClient instance
     */
    public RedisClient getRedisClient() {
        return redisClient;
    }

    /**
     * @return the RedisClusterClient instance
     */
    public RedisClusterClient getRedisClusterClient() {
        return redisClusterClient;
    }

    /**
     * Shutdown the clients.
     */
    public void destroy() {
        if (redisClient != null) {
            redisClient.shutdown();
            redisClient = null;
        }
        if (redisClusterClient != null) {
            redisClusterClient.shutdown();
            redisClusterClient = null;
        }
    }

}
