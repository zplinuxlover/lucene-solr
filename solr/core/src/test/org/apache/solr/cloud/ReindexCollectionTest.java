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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.api.collections.ReindexCollectionCmd;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.api.collections.ReindexCollectionCmd=DEBUG")
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

    TestInjection.reset();
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

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", targetCollection, (liveNodes, coll) -> {
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

    String realTargetCollection = null;
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, cloudManager.getTimeSource());
    String prefix = ReindexCollectionCmd.TARGET_COL_PREFIX + targetCollection;
    while (!timeOut.hasTimedOut()) {
      timeOut.sleep(500);
      for (String name : cloudManager.getClusterStateProvider().getClusterState().getCollectionsMap().keySet()) {
        if (name.startsWith(prefix)) {
          realTargetCollection = name;
          break;
        }
      }
      if (realTargetCollection != null) {
        break;
      }
    }
    assertNotNull("target collection not present after 30s", realTargetCollection);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", realTargetCollection, (liveNodes, coll) -> {
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

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", targetCollection, (liveNodes, coll) -> {
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
    final String targetCollection = "reshapeReindexingTarget";
    createCollection(sourceCollection, "conf1", 2, 2);
    indexDocs(sourceCollection, NUM_DOCS,
        i -> new SolrInputDocument(
            "id", String.valueOf(i),
            "string_s", String.valueOf(i),
            "remove_s", String.valueOf(i)));

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection)
        .setCollectionParam(ZkStateReader.NUM_SHARDS_PROP, 3)
        .setCollectionParam(ZkStateReader.REPLICATION_FACTOR, 1)
        .setCollectionParam("router.name", ImplicitDocRouter.NAME)
        .setCollectionParam("shards", "foo,bar,baz")
        .setCollectionParam("fl", "id,string_s")
        .setCollectionParam("q", "id:10*");
    req.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", targetCollection, (liveNodes, coll) -> {
      ReindexCollectionCmd.State state = ReindexCollectionCmd.State.get(coll.getStr(ReindexCollectionCmd.REINDEXING_PROP));
      return ReindexCollectionCmd.State.FINISHED == state;
    });
    // verify the target docs exist
    QueryResponse rsp = solrClient.query(targetCollection, params(CommonParams.Q, "*:*"));
    // 10 and 100-109
    assertEquals("copied num docs", 11, rsp.getResults().getNumFound());
    // verify the correct fields exist
    for (SolrDocument doc : rsp.getResults()) {
      assertNotNull(doc.getFieldValue("id"));
      assertNotNull(doc.getFieldValue("string_s"));
      assertNull(doc.getFieldValue("remove_s"));
    }

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

  @Test
  public void testFailure() throws Exception {
    final String sourceCollection = "failReindexing";
    final String targetCollection = "failReindexingTarget";
    final String aliasTarget = "failAlias";
    createCollection(sourceCollection, "conf1", 2, 2);
    createCollection(targetCollection, "conf1", 1, 1);
    CollectionAdminRequest.createAlias(aliasTarget, targetCollection).process(solrClient);
    indexDocs(sourceCollection, NUM_DOCS,
        i -> new SolrInputDocument(
            "id", String.valueOf(i),
            "string_s", String.valueOf(i)));

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection);
    try {
      req.process(solrClient);
      fail("succeeded but expected reindexing to fail due to the target collection already present");
    } catch (Exception e) {
      assertTrue(e instanceof BaseHttpSolrClient.RemoteSolrException);
      BaseHttpSolrClient.RemoteSolrException rse = (BaseHttpSolrClient.RemoteSolrException)e;
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, rse.code());
    }
    req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(aliasTarget);
    try {
      req.process(solrClient);
      fail("succeeded but expected reindexing to fail due to the target collection already present");
    } catch (Exception e) {
      assertTrue(e instanceof BaseHttpSolrClient.RemoteSolrException);
      BaseHttpSolrClient.RemoteSolrException rse = (BaseHttpSolrClient.RemoteSolrException)e;
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, rse.code());
    }

    CollectionAdminRequest.deleteAlias(aliasTarget).process(solrClient);
    CollectionAdminRequest.deleteCollection(targetCollection).process(solrClient);

    req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection);

    TestInjection.reindexFailure = "true:100";
    try {
      req.process(solrClient);
      fail("succeeded but expected reindexing to fail due to a test-injected failure");
    } catch (Exception e) {
      assertTrue(e instanceof BaseHttpSolrClient.RemoteSolrException);
      BaseHttpSolrClient.RemoteSolrException rse = (BaseHttpSolrClient.RemoteSolrException)e;
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, rse.code());
    }
    // verify that the target and checkpoint collections don't exist
    cloudManager.getClusterStateProvider().getClusterState().forEachCollection(coll -> {
      assertFalse(coll.getName() + " still exists", coll.getName().startsWith(ReindexCollectionCmd.TARGET_COL_PREFIX));
      assertFalse(coll.getName() + " still exists", coll.getName().startsWith(ReindexCollectionCmd.CHK_COL_PREFIX));
    });
    // verify that the source collection is read-write and has no reindexing flags
    CloudTestUtils.waitForState(cloudManager, "collection state is incorrect", sourceCollection,
        ((liveNodes, collectionState) ->
            !collectionState.isReadOnly() &&
            collectionState.getStr(ReindexCollectionCmd.REINDEXING_PROP) == null));
  }

  @Test
  public void testAbort() throws Exception {
    final String sourceCollection = "abortReindexing";
    final String targetCollection = "abortReindexingTarget";
    createCollection(sourceCollection, "conf1", 2, 1);

    TestInjection.reindexLatch = new CountDownLatch(1);
    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection);
    String asyncId = req.processAsync(solrClient);
    // wait for the source collection to be put in readOnly mode
    CloudTestUtils.waitForState(cloudManager, "source collection didn't become readOnly",
        sourceCollection, (liveNodes, coll) -> coll.isReadOnly());

    req = CollectionAdminRequest.reindexCollection(sourceCollection);
    req.setAbort(true);
    req.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "incorrect collection state", sourceCollection,
        ((liveNodes, collectionState) ->
            collectionState.isReadOnly() &&
            ReindexCollectionCmd.State.ABORTED.toLower().equals(collectionState.getStr(ReindexCollectionCmd.REINDEXING_PROP))));
    // let the process continue
    TestInjection.reindexLatch.countDown();
    CloudTestUtils.waitForState(cloudManager, "source collection is in wrong state",
        sourceCollection, (liveNodes, docCollection) -> {
          System.err.println("-- coll " + docCollection);
          return !docCollection.isReadOnly() && docCollection.getStr(ReindexCollectionCmd.REINDEXING_PROP) == null;
        });
    // verify the response
    CollectionAdminRequest.RequestStatusResponse rsp = CollectionAdminRequest.requestStatus(asyncId).process(solrClient);
    rsp.getRequestStatus();
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
