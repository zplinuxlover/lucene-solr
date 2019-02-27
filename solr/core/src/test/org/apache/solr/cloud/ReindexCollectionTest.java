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
import java.util.List;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.api.collections.ReindexCollectionCmd;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
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
        .addConfig("conf1", configset("cloud-dynamic"))
        .addConfig("conf2", configset("cloud-minimal"))
        .configure();
  }

  private CloudSolrClient solrClient;
  private SolrCloudManager cloudManager;

  @Before
  public void doBefore() throws Exception {
    solrClient = getCloudSolrClient(cluster);
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
  }

  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections(); // deletes aliases too

    solrClient.close();
  }

  private static final int NUM_DOCS = 200;

  @Test
  public void testBasicReindexing() throws Exception {
    final String sourceCollection = "basicReindexing";

    CollectionAdminRequest.createCollection(sourceCollection, "conf1", 2, 2)
        .setMaxShardsPerNode(-1)
        .process(solrClient);

    cluster.waitForActiveCollection(sourceCollection, 2, 4);

    // verify that indexing works
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(sourceCollection, docs);
    solrClient.commit(sourceCollection);
    // verify the docs exist
    QueryResponse rsp = solrClient.query(sourceCollection, params(CommonParams.Q, "*:*"));
    assertEquals("initial num docs", NUM_DOCS, rsp.getResults().getNumFound());

    final String targetCollection = "basicReindexingTarget";

    CollectionAdminRequest.ReindexCollection req = CollectionAdminRequest.reindexCollection(sourceCollection)
        .setTarget(targetCollection);
    req.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, "did not finish copying in time", sourceCollection, (liveNodes, coll) -> {
      ReindexCollectionCmd.State state = ReindexCollectionCmd.State.get(coll.getStr(ReindexCollectionCmd.REINDEXING_PROP));
      return ReindexCollectionCmd.State.FINISHED == state;
    });
    // verify the target docs exist
    rsp = solrClient.query(targetCollection, params(CommonParams.Q, "*:*"));
    assertEquals("copied num docs", NUM_DOCS, rsp.getResults().getNumFound());
  }
}
