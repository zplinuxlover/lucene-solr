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

package org.apache.solr.cloud;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.api.collections.ReindexCollectionCmd;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class ReindexCollectionTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        // only *_s
        .addConfig("conf1", configset("cloud-minimal"))
        // every combination of field flags
        .addConfig("conf2", configset("cloud-dynamic"))
        // catch-all * field, indexed+stored
        .addConfig("conf3", configset("cloud-minimal-inplace-updates"))
        .configure();
  }

  private CloudSolrClient solrClient;
  private SolrCloudManager cloudManager;

  @Before
  public void doBefore() throws Exception {
    ZkController zkController = cluster.getJettySolrRunner(0).getCoreContainer().getZkController();
    cloudManager = zkController.getSolrCloudManager();
    solrClient = new CloudSolrClientBuilder(Collections.singletonList(zkController.getZkServerAddress()),
        Optional.empty()).build();
  }

  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections(); // deletes aliases too

    solrClient.close();
  }

  private static final int NUM_DOCS = 200; // at least two batches, default batchSize=100

  @Test
  public void testBasicReindexing() throws Exception {
    final String sourceCollection = "basicReindexing";

    createCollection(sourceCollection, "conf1", 2, 2);

    indexDocs(sourceCollection, NUM_DOCS,
        i -> new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));

    final String targetCollection = "basicReindexingTarget";

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection);
    req.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", sourceCollection, (liveNodes, coll) -> {
      ReindexCollectionCmd.State state = ReindexCollectionCmd.State.get(coll.getStr(ReindexCollectionCmd.REINDEXING_PROP));
      return ReindexCollectionCmd.State.FINISHED == state;
    });
    // verify the target docs exist
    QueryResponse rsp = solrClient.query(targetCollection, params(CommonParams.Q, "*:*"));
    assertEquals("copied num docs", NUM_DOCS, rsp.getResults().getNumFound());
  }

  public void testSameTargetReindexing() throws Exception {
    final String sourceCollection = "sameTargetReindexing";
    final String targetCollection = sourceCollection;

    createCollection(sourceCollection, "conf1", 2, 2);
    indexDocs(sourceCollection, NUM_DOCS,
        i -> new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection);
    req.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", sourceCollection, (liveNodes, coll) -> {
      ReindexCollectionCmd.State state = ReindexCollectionCmd.State.get(coll.getStr(ReindexCollectionCmd.REINDEXING_PROP));
      return ReindexCollectionCmd.State.FINISHED == state;
    });
    // verify the target docs exist
    QueryResponse rsp = solrClient.query(targetCollection, params(CommonParams.Q, "*:*"));
    assertEquals("copied num docs", NUM_DOCS, rsp.getResults().getNumFound());
  }

  @Test
  public void testLossySchema() throws Exception {
    final String sourceCollection = "sourceLossyReindexing";
    final String targetCollection = "targetLossyReindexing";


    createCollection(sourceCollection, "conf2", 2, 2);

    indexDocs(sourceCollection, NUM_DOCS, i ->
      new SolrInputDocument(
          "id", String.valueOf(i),
          "string_s", String.valueOf(i),
          "sind", "this is a test " + i)); // "sind": indexed=true, stored=false, will be lost...

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection)
        .setConfigName("conf3");
    req.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", sourceCollection, (liveNodes, coll) -> {
      ReindexCollectionCmd.State state = ReindexCollectionCmd.State.get(coll.getStr(ReindexCollectionCmd.REINDEXING_PROP));
      return ReindexCollectionCmd.State.FINISHED == state;
    });
    // verify the target docs exist
    QueryResponse rsp = solrClient.query(targetCollection, params(CommonParams.Q, "*:*"));
    assertEquals("copied num docs", NUM_DOCS, rsp.getResults().getNumFound());
    for (SolrDocument doc : rsp.getResults()) {
      String id = (String)doc.getFieldValue("id");
      assertEquals(id, doc.getFieldValue("string_s"));
      assertFalse(doc.containsKey("sind")); // lost in translation ...
    }
  }

  @Test
  public void testReshapeReindexing() throws Exception {
    final String sourceCollection = "reshapeReindexing";
    final String targetCollection = sourceCollection;
    createCollection(sourceCollection, "conf1", 2, 2);
    indexDocs(sourceCollection, NUM_DOCS,
        i -> new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection)
        .setCollectionParam(ZkStateReader.NUM_SHARDS_PROP, 3)
        .setCollectionParam(ZkStateReader.REPLICATION_FACTOR, 1)
        .setCollectionParam("router.name", ImplicitDocRouter.NAME)
        .setCollectionParam("shards", "foo,bar,baz");
    req.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", sourceCollection, (liveNodes, coll) -> {
      ReindexCollectionCmd.State state = ReindexCollectionCmd.State.get(coll.getStr(ReindexCollectionCmd.REINDEXING_PROP));
      return ReindexCollectionCmd.State.FINISHED == state;
    });
    // verify the target docs exist
    QueryResponse rsp = solrClient.query(targetCollection, params(CommonParams.Q, "*:*"));
    assertEquals("copied num docs", NUM_DOCS, rsp.getResults().getNumFound());

    // check the shape of the new collection
    ClusterState clusterState = solrClient.getClusterStateProvider().getClusterState();
    List<String> aliases = solrClient.getZkStateReader().getAliases().resolveAliases(targetCollection);
    assertFalse(aliases.isEmpty());
    String realTargetCollection = aliases.get(0);
    DocCollection coll = clusterState.getCollection(realTargetCollection);
    assertNotNull(coll);
    assertEquals(3, coll.getSlices().size());
    assertNotNull("foo", coll.getSlice("foo"));
    assertNotNull("bar", coll.getSlice("bar"));
    assertNotNull("baz", coll.getSlice("baz"));
    assertEquals(new Integer(1), coll.getReplicationFactor());
    assertEquals(ImplicitDocRouter.NAME, coll.getRouter().getName());
  }

  private void createCollection(String name, String config, int numShards, int numReplicas) throws Exception {
    CollectionAdminRequest.createCollection(name, config, numShards, numReplicas)
        .setMaxShardsPerNode(-1)
        .process(solrClient);

    cluster.waitForActiveCollection(name, numShards, numShards * numReplicas);
  }

  private void indexDocs(String collection, int numDocs, Function<Integer, SolrInputDocument> generator) throws Exception {
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      docs.add(generator.apply(i));
    }
    solrClient.add(collection, docs);
    solrClient.commit(collection);
    // verify the docs exist
    QueryResponse rsp = solrClient.query(collection, params(CommonParams.Q, "*:*"));
    assertEquals("num docs", NUM_DOCS, rsp.getResults().getNumFound());

  }
}
