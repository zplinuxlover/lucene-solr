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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.util.IdUtils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SystemCollectionCompatTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf1", configset("cloud-minimal"))
        .configure();
    if (! log.isWarnEnabled()) {
      fail("Test requires that log-level is at-least WARN, but WARN is disabled");
    }
  }

  private SolrCloudManager cloudManager;
  private CloudSolrClient solrClient;

  @Before
  public void setupSystemCollection() throws Exception {
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 3)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(CollectionAdminParams.SYSTEM_COLL,  1, 3);
    ZkController zkController = cluster.getJettySolrRunner(0).getCoreContainer().getZkController();
    cloudManager = zkController.getSolrCloudManager();
    solrClient = new CloudSolrClientBuilder(Collections.singletonList(zkController.getZkServerAddress()),
        Optional.empty()).build();
    // send a dummy doc to the .system collection
    SolrInputDocument doc = new SolrInputDocument(
        "id", IdUtils.timeRandomId(),
        CommonParams.TYPE, "dummy");
    doc.addField("time_l", cloudManager.getTimeSource().getEpochTimeNs());
    doc.addField("timestamp", new Date());
    solrClient.add(CollectionAdminParams.SYSTEM_COLL, doc);
    solrClient.commit(CollectionAdminParams.SYSTEM_COLL);

    // workaround for a bug in schema update API
    String pathToBackup = ZkStateReader.CONFIGS_ZKNODE + "/.system/schema.xml.bak";
    DistribStateManager stateManager = cloudManager.getDistribStateManager();
    VersionedData data = null;
    if (stateManager.hasData(pathToBackup)) {
      data = stateManager.getData(pathToBackup);
    }
    String path = ZkStateReader.CONFIGS_ZKNODE + "/.system/schema.xml";
    if (!stateManager.hasData(path) && data != null) {
      stateManager.createData(path, data.getData(), CreateMode.PERSISTENT);
      stateManager.setData(path, data.getData(), -1);
      stateManager.removeData(pathToBackup, -1);
      stateManager.createData(pathToBackup, data.getData(), CreateMode.PERSISTENT);
      // update it to increase the version
      stateManager.setData(pathToBackup, data.getData(), -1);
    }
    // trigger compat report by changing the schema
    SchemaRequest req = new SchemaRequest();
    SchemaResponse rsp = req.process(solrClient, CollectionAdminParams.SYSTEM_COLL);
    Map<String, Object> field = getSchemaField("timestamp", rsp);
    // make obviously incompatible changes
    field.put("type", "string");
    field.put("docValues", false);
    SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(field);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(solrClient, CollectionAdminParams.SYSTEM_COLL);
    assertEquals(replaceFieldResponse.toString(), 0, replaceFieldResponse.getStatus());
    // reload for the schema changes to become active
    CollectionAdminRequest.reloadCollection(CollectionAdminParams.SYSTEM_COLL);
    cluster.waitForActiveCollection(CollectionAdminParams.SYSTEM_COLL,  1, 3);
  }

  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections();

    solrClient.close();
  }

  private Map<String, Object> getSchemaField(String name, SchemaResponse schemaResponse) {
    List<Map<String, Object>> fields = schemaResponse.getSchemaRepresentation().getFields();
    for (Map<String, Object> field : fields) {
      if (name.equals(field.get("name"))) {
        return field;
      }
    }
    return null;
  }

  @Test
  public void testBackCompat() throws Exception {
    CollectionAdminRequest.OverseerStatus status = new CollectionAdminRequest.OverseerStatus();
    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminResponse adminResponse = status.process(solrClient);
    NamedList<Object> response = adminResponse.getResponse();
    String leader = (String) response.get("leader");
    JettySolrRunner overseerNode = null;
    int index = -1;
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner runner = jettySolrRunners.get(i);
      if (runner.getNodeName().equals(leader)) {
        overseerNode = runner;
        index = i;
        break;
      }
    }
    assertNotNull(overseerNode);
    LogWatcherConfig watcherCfg = new LogWatcherConfig(true, null, "WARN", 100);
    LogWatcher watcher = LogWatcher.newRegisteredLogWatcher(watcherCfg, null);

    watcher.reset();

    // restart Overseer to trigger the back-compat check
    cluster.stopJettySolrRunner(index);
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, cloudManager.getTimeSource());
    while (!timeOut.hasTimedOut()) {
      adminResponse = status.process(solrClient);
      response = adminResponse.getResponse();
      String newLeader = (String) response.get("leader");
      if (newLeader != null && !leader.equals(newLeader)) {
        break;
      }
      timeOut.sleep(200);
    }
    if (timeOut.hasTimedOut()) {
      fail("time out waiting for new Overseer leader");
    }

    Thread.sleep(5000);
    SolrDocumentList history = watcher.getHistory(-1, null);
    assertFalse(history.isEmpty());
    boolean foundWarning = false;
    boolean foundSchemaWarning = false;
  }

}
