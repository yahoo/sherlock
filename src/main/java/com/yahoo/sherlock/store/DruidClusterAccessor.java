/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.exception.ClusterNotFoundException;

import lombok.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The {@code DruidClusterAccessor} defines an interface for
 * communicating with a persistence layer, be it SQL, Redis, etc.,
 * to retrieve and store {@code DruidCluster} objects.
 */
public interface DruidClusterAccessor {

    /**
     * Get the {@code DruidCluster} object with the specified ID.
     *
     * @param clusterId the ID for which to retrieve the druid cluster
     * @return the cluster with the specified ID
     * @throws IOException              if there is an error with the persistence layer
     * @throws ClusterNotFoundException if no cluster can be found with the specified ID
     */
    @NonNull
    DruidCluster getDruidCluster(String clusterId) throws IOException, ClusterNotFoundException;

    /**
     * Get a cluster with an ID integer val.
     *
     * @param clusterId cluster ID
     * @return the associated cluster
     * @throws IOException              if an error occurs with the backend
     * @throws ClusterNotFoundException if the cluster does not exist
     */
    @NonNull
    default DruidCluster getDruidCluster(Integer clusterId) throws IOException, ClusterNotFoundException {
        return getDruidCluster(clusterId.toString());
    }

    /**
     * Put a {@code DruidCluster} object in the store.
     * If the Druid cluster ID is null, then a new ID should be
     * assigned and the new cluster should be inserted. Otherwise
     * the cluster with the existing ID should be overridden.
     * The method will overwrite existing objects.
     *
     * @param cluster the cluster to store
     * @throws IOException if there is an error with the persistence layer
     */
    void putDruidCluster(DruidCluster cluster) throws IOException;

    /**
     * Delete a {@code DruidCluster} from the store.
     *
     * @param clusterId the ID for which to delete the cluster
     * @throws IOException              if there is an error with the persistence layer
     * @throws ClusterNotFoundException if no cluster can be found with the specified ID
     */
    void deleteDruidCluster(String clusterId) throws IOException, ClusterNotFoundException;

    /**
     * Get a {@code List} of all {@code DruidCluster} objects in the store.
     *
     * @return a list of clusters, which may be empty
     * @throws IOException if there is an error with the persistence layer
     */
    @NonNull
    List<DruidCluster> getDruidClusterList() throws IOException;

    /**
     * Get all druid cluster Ids.
     * @return set of druid cluster Ids
     */
    Set<String> getDruidClusterIds();

    /**
     * Remove cluster Id from cluster index.
     *
     * @param clusterId cluster Id
     */
    void removeFromClusterIdIndex(String clusterId);
}
