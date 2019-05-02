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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;

/**
 * Read-only snapshot of another {@link NodeStateProvider}.
 */
public class SnapshotNodeStateProvider implements NodeStateProvider {
  private Map<String, Map<String, Object>> nodeValues = new HashMap<>();
  private Map<String, Map<String, Map<String, List<ReplicaInfo>>>> replicaInfos = new HashMap<>();

  public static final List<String> REPLICA_TAGS = Arrays.asList(
      Variable.Type.CORE_IDX.metricsAttribute,
      "QUERY./select.requests",
      "UPDATE./update.requests"
  );
  public static final Set<String> NODE_TAGS = SimCloudManager.createNodeValues("unused:1234_solr").keySet();


  public SnapshotNodeStateProvider(SolrCloudManager other) {
    for (String node : other.getClusterStateProvider().getLiveNodes()) {
      nodeValues.put(node, other.getNodeStateProvider().getNodeValues(node, NODE_TAGS));
      Map<String, Map<String, List<ReplicaInfo>>> infos = other.getNodeStateProvider().getReplicaInfo(node, REPLICA_TAGS);
      infos.forEach((collection, shards) -> {
        shards.forEach((shard, replicas) -> {
          replicas.forEach(r -> {
            List<ReplicaInfo> myReplicas = replicaInfos
                .computeIfAbsent(node, n -> new HashMap<>())
                .computeIfAbsent(collection, c -> new HashMap<>())
                .computeIfAbsent(shard, s -> new ArrayList<>());
            Map<String, Object> rMap = new HashMap<>();
            r.toMap(rMap);
            if (r.isLeader) { // ReplicaInfo.toMap doesn't write this!!!
              ((Map<String, Object>)rMap.values().iterator().next()).put("leader", "true");
            }
            myReplicas.add(new ReplicaInfo(rMap));
          });
        });
      });
    }
  }

  public SnapshotNodeStateProvider(Map<String, Object> snapshot) {
    Objects.requireNonNull(snapshot);
    nodeValues = (Map<String, Map<String, Object>>)snapshot.getOrDefault("nodeValues", Collections.emptyMap());
    ((Map<String, Object>)snapshot.getOrDefault("replicaInfos", Collections.emptyMap())).forEach((node, v) -> {
      Map<String, Map<String, List<ReplicaInfo>>> perNode = replicaInfos.computeIfAbsent(node, n -> new HashMap<>());
      ((Map<String, Object>)v).forEach((collection, shards) -> {
        Map<String, List<ReplicaInfo>> perColl = perNode.computeIfAbsent(collection, c -> new HashMap<>());
        ((Map<String, Object>)shards).forEach((shard, replicas) -> {
          List<ReplicaInfo> infos = perColl.computeIfAbsent(shard, s -> new ArrayList<>());
          ((List<Map<String, Object>>)replicas).forEach(replicaMap -> {
            ReplicaInfo ri = new ReplicaInfo(new HashMap<>(replicaMap)); // constructor modifies this map
            infos.add(ri);
          });
        });
      });
    });
  }

  public Map<String, Object> getSnapshot() {
    Map<String, Object> snapshot = new HashMap<>();
    snapshot.put("nodeValues", nodeValues);
    Map<String, Map<String, Map<String, List<Map<String, Object>>>>> replicaInfosMap = new HashMap<>();
    snapshot.put("replicaInfos", replicaInfosMap);
    replicaInfos.forEach((node, perNode) -> {
      perNode.forEach((collection, shards) -> {
        shards.forEach((shard, replicas) -> {
          replicas.forEach(r -> {
            List<Map<String, Object>> myReplicas = replicaInfosMap
                .computeIfAbsent(node, n -> new HashMap<>())
                .computeIfAbsent(collection, c -> new HashMap<>())
                .computeIfAbsent(shard, s -> new ArrayList<>());
            Map<String, Object> rMap = new HashMap<>();
            r.toMap(rMap);
            if (r.isLeader) { // ReplicaInfo.toMap doesn't write this!!!
              ((Map<String, Object>)rMap.values().iterator().next()).put("leader", "true");
            }
            myReplicas.add(rMap);
          });
        });
      });
    });
    return snapshot;
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    return nodeValues.getOrDefault(node, Collections.emptyMap());
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return replicaInfos.getOrDefault(node, Collections.emptyMap());
  }

  @Override
  public void close() throws IOException {

  }
}
