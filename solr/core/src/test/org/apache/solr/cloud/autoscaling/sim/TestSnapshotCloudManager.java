package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestSnapshotCloudManager extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static int NODE_COUNT = 3;

  private static SolrCloudManager realManager;

  // set up a real cluster as the source of test data
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2, 0, 1)
        .process(cluster.getSolrClient());
    realManager = cluster.getJettySolrRunner(cluster.getJettySolrRunners().size() - 1).getCoreContainer()
        .getZkController().getSolrCloudManager();
  }

  @Test
  public void testSnapshots() throws Exception {
    SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager);
    Map<String, Object> snapshot = snapshotCloudManager.getSnapshot();
    SnapshotCloudManager snapshotCloudManager1 = new SnapshotCloudManager(snapshot);
    assertClusterStateEquals(realManager.getClusterStateProvider().getClusterState(), snapshotCloudManager.getClusterStateProvider().getClusterState());
    assertClusterStateEquals(realManager.getClusterStateProvider().getClusterState(), snapshotCloudManager1.getClusterStateProvider().getClusterState());
    // this will always fail because the metrics will be already different
    // assertNodeStateProvider(realManager, snapshotCloudManager);
    assertNodeStateProvider(snapshotCloudManager, snapshotCloudManager1);
    assertDistribStateManager(snapshotCloudManager.getDistribStateManager(), snapshotCloudManager1.getDistribStateManager());
  }

  private static void assertNodeStateProvider(SolrCloudManager oneMgr, SolrCloudManager twoMgr) throws Exception {
    NodeStateProvider one = oneMgr.getNodeStateProvider();
    NodeStateProvider two = twoMgr.getNodeStateProvider();
    for (String node : oneMgr.getClusterStateProvider().getLiveNodes()) {
      Map<String, Object> oneVals = one.getNodeValues(node, SnapshotNodeStateProvider.NODE_TAGS);
      Map<String, Object> twoVals = two.getNodeValues(node, SnapshotNodeStateProvider.NODE_TAGS);
      assertEquals(oneVals, twoVals);
      Map<String, Map<String, List<ReplicaInfo>>> oneInfos = one.getReplicaInfo(node, SnapshotNodeStateProvider.REPLICA_TAGS);
      Map<String, Map<String, List<ReplicaInfo>>> twoInfos = two.getReplicaInfo(node, SnapshotNodeStateProvider.REPLICA_TAGS);
      assertEquals(oneInfos, twoInfos);
    }
  }

  private static void assertDistribStateManager(DistribStateManager one, DistribStateManager two) throws Exception {
    List<String> treeOne = new ArrayList<>(one.listTree("/"));
    List<String> treeTwo = new ArrayList<>(two.listTree("/"));
    Collections.sort(treeOne);
    Collections.sort(treeTwo);
    assertEquals(treeOne, treeTwo);
    for (String path : treeOne) {
      VersionedData vd1 = one.getData(path);
      VersionedData vd2 = two.getData(path);
      assertEquals(vd1, vd2);
    }
  }

  private static void assertClusterStateEquals(ClusterState one, ClusterState two) {
    assertEquals(one.getLiveNodes(), two.getLiveNodes());
    assertEquals(one.getCollectionsMap().keySet(), two.getCollectionsMap().keySet());
    one.forEachCollection(oneColl -> {
      DocCollection twoColl = two.getCollection(oneColl.getName());
      Map<String, Slice> oneSlices = oneColl.getSlicesMap();
      Map<String, Slice> twoSlices = twoColl.getSlicesMap();
      assertEquals(oneSlices.keySet(), twoSlices.keySet());
      oneSlices.forEach((s, slice) -> {
        Slice sTwo = twoSlices.get(s);
        for (Replica oneReplica : slice.getReplicas()) {
          Replica twoReplica = sTwo.getReplica(oneReplica.getName());
          assertNotNull(twoReplica);
          assertReplicaEquals(oneReplica, twoReplica);
        }
      });
    });
  }

  private static void assertReplicaEquals(Replica one, Replica two) {
    assertEquals(one.getName(), two.getName());
    assertEquals(one.getNodeName(), two.getNodeName());
    assertEquals(one.getState(), two.getState());
    assertEquals(one.getType(), two.getType());
    Map<String, Object> filteredPropsOne = one.getProperties().entrySet().stream()
        .filter(e -> !(e.getKey().startsWith("INDEX") || e.getKey().startsWith("QUERY") || e.getKey().startsWith("UPDATE")))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<String, Object> filteredPropsTwo = two.getProperties().entrySet().stream()
        .filter(e -> !(e.getKey().startsWith("INDEX") || e.getKey().startsWith("QUERY") || e.getKey().startsWith("UPDATE")))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    assertEquals(filteredPropsOne, filteredPropsTwo);
  }

}
