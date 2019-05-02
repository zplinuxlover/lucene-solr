/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;

/**
 * Read-only snapshot of another {@link SolrCloudManager}.
 */
public class SnapshotCloudManager implements SolrCloudManager {
  private ObjectCache objectCache = new ObjectCache();
  private SnapshotClusterStateProvider clusterStateProvider;
  private SnapshotNodeStateProvider nodeStateProvider;
  private SnapshotDistribStateManager distribStateManager;
  private TimeSource timeSource;

  public SnapshotCloudManager(SolrCloudManager other) throws Exception {
    this.timeSource = other.getTimeSource();
    this.clusterStateProvider = new SnapshotClusterStateProvider(other.getClusterStateProvider());
    this.nodeStateProvider = new SnapshotNodeStateProvider(other);
    this.distribStateManager = new SnapshotDistribStateManager(other.getDistribStateManager());
  }

  public SnapshotCloudManager(Map<String, Object> snapshot) {
    Objects.requireNonNull(snapshot);
    init(
        (Map<String, Object>)snapshot.getOrDefault("managerState", Collections.emptyMap()),
        (Map<String, Object>)snapshot.getOrDefault("clusterState", Collections.emptyMap()),
        (Map<String, Object>)snapshot.getOrDefault("nodeState", Collections.emptyMap()),
        (Map<String, Object>)snapshot.getOrDefault("distribState", Collections.emptyMap())
    );
  }

  private void init(Map<String, Object> managerState, Map<String, Object> clusterState, Map<String, Object> nodeState, Map<String, Object> distribState) {
    Objects.requireNonNull(managerState);
    Objects.requireNonNull(clusterState);
    Objects.requireNonNull(nodeState);
    Objects.requireNonNull(distribState);
    this.timeSource = TimeSource.get((String)managerState.getOrDefault("timeSource", "simTime:50"));
    this.clusterStateProvider = new SnapshotClusterStateProvider(clusterState);
    this.nodeStateProvider = new SnapshotNodeStateProvider(nodeState);
    this.distribStateManager = new SnapshotDistribStateManager(distribState);
  }

  public Map<String, Object> getSnapshot() {
    Map<String, Object> snapshot = new LinkedHashMap<>(4);
    Map<String, Object> managerState = new HashMap<>();
    managerState.put("timeSource", timeSource.toString());
    snapshot.put("managerState", managerState);

    snapshot.put("clusterState", clusterStateProvider.getSnapshot());
    snapshot.put("nodeState", nodeStateProvider.getSnapshot());
    snapshot.put("distribState", distribStateManager.getSnapshot());
    return snapshot;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return clusterStateProvider;
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return nodeStateProvider;
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return distribStateManager;
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return NoopDistributedQueueFactory.INSTANCE;
  }

  @Override
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return timeSource;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    throw new UnsupportedOperationException("request");
  }

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    throw new UnsupportedOperationException("httpRequest");
  }

  @Override
  public void close() throws IOException {

  }
}
