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

package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reindex a collection, usually in order to change the index schema.
 * <p>WARNING: Reindexing is a potentially lossy operation - some indexed data that is not available as
 * stored fields may be irretrievably lost, so users should use this command with caution, evaluating
 * the potential impact by using different source and target collection names first, and preserving
 * the source collection until the evaluation is complete.</p>
 * <p>Reindexing follows these steps:</p>
 * <ol>
 *    <li>create a temporary collection using the most recent schema of the source collection
 *    (or the one specified in the parameters, which must already exist).</li>
 *    <li>copy the source documents to the temporary collection, reconstructing them from their stored
 *    fields and reindexing them using the specified schema. NOTE: some data
 *    loss may occur if the original stored field data is not available!</li>
 *    <li>if the target collection name is not specified
 *    then the same name as the source is assumed and at this step the source collection is permanently removed.</li>
 *    <li>create the target collection from scratch with the specified name (or the same as source if not
 *    specified), but using the new specified schema. NOTE: if the target name was not specified or is the same
 *    as the source collection then the original collection has been deleted in the previous step and it's
 *    not possible to roll-back the changes if the process is interrupted. The (possibly incomplete) data
 *    is still available in the temporary collection.</li>
 *    <li>copy the documents from the temporary collection to the target collection, using the specified schema.</li>
 *    <li>delete temporary collection(s) and optionally delete the source collection if it still exists.</li>
 * </ol>
 */
public class ReindexCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ABORT = "abort";
  public static final String KEEP_SOURCE = "keepSource";
  public static final String TARGET = "target";
  public static final String TARGET_COL_PREFIX = ".reindex_";
  public static final String CHK_COL_PREFIX = ".reindex_ck_";
  public static final String REINDEXING_PROP = CollectionAdminRequest.PROPERTY_PREFIX + "reindexing";
  public static final String REINDEX_PHASE_PROP = CollectionAdminRequest.PROPERTY_PREFIX + "reindex_phase";

  private static final List<String> COLLECTION_PARAMS = Arrays.asList(
      ZkStateReader.CONFIGNAME_PROP,
      ZkStateReader.NUM_SHARDS_PROP,
      ZkStateReader.NRT_REPLICAS,
      ZkStateReader.PULL_REPLICAS,
      ZkStateReader.TLOG_REPLICAS,
      ZkStateReader.REPLICATION_FACTOR,
      ZkStateReader.MAX_SHARDS_PER_NODE,
      "shards",
      Policy.POLICY,
      CollectionAdminParams.CREATE_NODE_SET_PARAM,
      CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM,
      ZkStateReader.AUTO_ADD_REPLICAS
  );

  private final OverseerCollectionMessageHandler ocmh;

  private static AtomicInteger tmpCollectionSeq = new AtomicInteger();

  public enum State {
    IDLE,
    RUNNING,
    ABORTED,
    FINISHED;

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

    public static State get(String p) {
      if (p == null) {
        return null;
      }
      p = p.toLowerCase(Locale.ROOT);
      if (p.startsWith(CollectionAdminRequest.PROPERTY_PREFIX)) {
        p = p.substring(CollectionAdminRequest.PROPERTY_PREFIX.length());
      }
      return states.get(p);
    }
    static Map<String, State> states = Collections.unmodifiableMap(
        Stream.of(State.values()).collect(Collectors.toMap(State::toLower, Function.identity())));
  }

  public ReindexCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {

    log.info("*** called: {}", message);

    String collection = message.getStr(CommonParams.NAME);
    if (collection == null || clusterState.getCollectionOrNull(collection) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection name must be specified and must exist");
    }
    String target = message.getStr(TARGET);
    if (target == null) {
      target = collection;
    }
    boolean sameTarget = target.equals(collection);
    boolean keepSource = message.getBool(KEEP_SOURCE, true);
    boolean abort = message.getBool(ABORT, false);
    DocCollection coll = clusterState.getCollection(collection);
    if (abort) {
      ZkNodeProps props = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, collection,
          REINDEXING_PROP, State.ABORTED.toLower());
      ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
      results.add(State.ABORTED.toLower(), collection);
      // if needed the cleanup will be performed by the running instance of the command
      return;
    }
    // check it's not already running
    State state = State.get(coll.getStr(REINDEXING_PROP, State.IDLE.toLower()));
    if (state == State.RUNNING) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Reindex is already running for collection " + collection +
          ". If you are sure this is not the case you can issue &abort=true to clean up this state.");
    }
    // set the running flag
    ZkNodeProps props = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
        ZkStateReader.COLLECTION_PROP, collection,
        REINDEXING_PROP, State.RUNNING.toLower());
    ocmh.overseer.offerStateUpdate(Utils.toJSON(props));

    boolean aborted = false;
    int batchSize = message.getInt(CommonParams.ROWS, 100);
    String query = message.getStr(CommonParams.Q, "*:*");
    Integer rf = message.getInt(ZkStateReader.REPLICATION_FACTOR, coll.getReplicationFactor());
    Integer numNrt = message.getInt(ZkStateReader.NRT_REPLICAS, coll.getNumNrtReplicas());
    Integer numTlog = message.getInt(ZkStateReader.TLOG_REPLICAS, coll.getNumTlogReplicas());
    Integer numPull = message.getInt(ZkStateReader.PULL_REPLICAS, coll.getNumPullReplicas());
    int numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, coll.getActiveSlices().size());
    int maxShardsPerNode = message.getInt(ZkStateReader.MAX_SHARDS_PER_NODE, coll.getMaxShardsPerNode());
    DocRouter router = coll.getRouter();
    if (router == null) {
      router = DocRouter.DEFAULT;
    }

    String configName = message.getStr(ZkStateReader.CONFIGNAME_PROP, ocmh.zkStateReader.readConfigName(collection));
    int seq = tmpCollectionSeq.getAndIncrement();
    String targetCollection = sameTarget ?
        TARGET_COL_PREFIX + collection + "_" + seq : target;
    String chkCollection = CHK_COL_PREFIX + collection + "_" + seq;
    String daemonUrl = null;
    Exception exc = null;
    try {
      // 0. set up target and checkpoint collections
      NamedList<Object> cmdResults = new NamedList<>();
      ZkNodeProps cmd;
      if (clusterState.hasCollection(targetCollection)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Target collection " + targetCollection + " already exists! Delete it first.");
      }
      if (clusterState.hasCollection(chkCollection)) {
        // delete the checkpoint collection
        cmd = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
            CommonParams.NAME, chkCollection,
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
        // nocommit error checking
      }

      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower());
      propMap.put(CommonParams.NAME, targetCollection);
      propMap.put(ZkStateReader.NUM_SHARDS_PROP, numShards);
      propMap.put(CollectionAdminParams.COLL_CONF, configName);
      // init first from the same router
      propMap.put("router.name", router.getName());
      for (String key : coll.keySet()) {
        if (key.startsWith("router.")) {
          propMap.put(key, coll.get(key));
        }
      }
      // then apply overrides if present
      for (String key : message.keySet()) {
        if (key.startsWith("router.")) {
          propMap.put(key, message.getStr(key));
        } else if (COLLECTION_PARAMS.contains(key)) {
          propMap.put(key, message.get(key));
        }
      }

      propMap.put(ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode);
      propMap.put(CommonAdminParams.WAIT_FOR_FINAL_STATE, true);
      if (rf != null) {
        propMap.put(ZkStateReader.REPLICATION_FACTOR, rf);
      }
      if (numNrt != null) {
        propMap.put(ZkStateReader.NRT_REPLICAS, numNrt);
      }
      if (numTlog != null) {
        propMap.put(ZkStateReader.TLOG_REPLICAS, numTlog);
      }
      if (numPull != null) {
        propMap.put(ZkStateReader.PULL_REPLICAS, numPull);
      }
      // create the target collection
      cmd = new ZkNodeProps(propMap);
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, cmd, cmdResults);
      // nocommit error checking

      // create the checkpoint collection - use RF=1 and 1 shard
      cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
          CommonParams.NAME, chkCollection,
          ZkStateReader.NUM_SHARDS_PROP, "1",
          ZkStateReader.REPLICATION_FACTOR, "1",
          CollectionAdminParams.COLL_CONF, "_default",
          CommonAdminParams.WAIT_FOR_FINAL_STATE, "true"
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, cmd, cmdResults);
      // nocommit error checking
      // wait for a while until we see both collections
      TimeOut waitUntil = new TimeOut(30, TimeUnit.SECONDS, ocmh.timeSource);
      boolean created = false;
      while (!waitUntil.hasTimedOut()) {
        waitUntil.sleep(100);
        // this also refreshes our local var clusterState
        clusterState = ocmh.cloudManager.getClusterStateProvider().getClusterState();
        created = clusterState.hasCollection(targetCollection) && clusterState.hasCollection(chkCollection);
        if (created) break;
      }
      if (!created) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not fully create temporary collection(s)");
      }
      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      // 1. put the source collection in read-only mode
      cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.READ_ONLY, "true");
      ocmh.overseer.offerStateUpdate(Utils.toJSON(props));

      // 2. copy the documents to target
      // Recipe taken from: http://joelsolr.blogspot.com/2016/10/solr-63-batch-jobs-parallel-etl-and.html
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      q.set("collection", collection);
      q.set("expr",
          "daemon(id=\"" + targetCollection + "\"," +
              "terminate=\"true\"," +
              "commit(" + targetCollection + "," +
                "update(" + targetCollection + "," +
                  "batchSize=" + batchSize + "," +
                  "topic(" + chkCollection + "," +
                    collection + "," +
                    "q=\"" + query + "\"," +
                    "fl=\"*\"," +
                    "id=\"topic_" + targetCollection + "\"," +
                    // some of the documents eg. in .system contain large blobs
                    "rows=\"" + batchSize + "\"," +
                    "initialCheckpoint=\"0\"))))");
      log.info("- starting copying documents from " + collection + " to " + targetCollection);
      SolrResponse rsp = null;
      try {
        rsp = ocmh.cloudManager.request(new QueryRequest(q));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to copy documents from " +
            collection + " to " + targetCollection, e);
      }
      daemonUrl = getDaemonUrl(rsp, coll);
      if (daemonUrl == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to copy documents from " +
            collection + " to " + targetCollection + ": " + Utils.toJSONString(rsp));
      }

      // wait for the daemon to finish
      waitForDaemon(targetCollection, daemonUrl, collection);
      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }
      log.info("- finished copying from " + collection + " to " + targetCollection);

      // 5. if (sameTarget) set up an alias to use targetCollection as the source name
      if (sameTarget) {
        cmd = new ZkNodeProps(
            CommonParams.NAME, collection,
            "collections", targetCollection);
        ocmh.commandMap.get(CollectionParams.CollectionAction.CREATEALIAS).call(clusterState, cmd, results);
      }

      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }
      // 6. delete the checkpoint collection
      cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
          CommonParams.NAME, chkCollection,
          CoreAdminParams.DELETE_METRICS_HISTORY, "true"
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);

      // nocommit error checking

      // 7. optionally delete the source collection
      if (keepSource) {
        // 8. set the FINISHED state and clear readOnly
        props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
            ZkStateReader.COLLECTION_PROP, collection,
            REINDEXING_PROP, State.FINISHED.toLower(),
            ZkStateReader.READ_ONLY, "");
        ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
      } else {
        cmd = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
            CommonParams.NAME, collection,
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
      }

      results.add(State.FINISHED.toLower(), collection);
    } catch (Exception e) {
      log.warn("Error during reindexing of " + collection, e);
      exc = e;
      aborted = true;
      throw e;
    } finally {
      if (aborted) {
        cleanup(collection, targetCollection, chkCollection, daemonUrl, targetCollection);
        results.add(State.ABORTED.toLower(), collection);
        if (exc != null) {
          results.add("error", exc.toString());
        }
      }
    }
  }

  private boolean maybeAbort(String collection) throws Exception {
    DocCollection coll = ocmh.cloudManager.getClusterStateProvider().getClusterState().getCollectionOrNull(collection);
    if (coll == null) {
      // collection no longer present - abort
      return true;
    }
    State state = State.get(coll.getStr(REINDEXING_PROP, State.RUNNING.toLower()));
    if (state != State.ABORTED) {
      return false;
    }
    return true;
  }

  // XXX see #waitForDaemon() for why we need this
  private String getDaemonUrl(SolrResponse rsp, DocCollection coll) {
    Map<String, Object> rs = (Map<String, Object>)rsp.getResponse().get("result-set");
    if (rs == null || rs.isEmpty()) {
      log.debug("Missing daemon information in response: " + Utils.toJSONString(rsp));
    }
    List<Object> list = (List<Object>)rs.get("docs");
    if (list == null) {
      log.debug("Missing daemon information in response: " + Utils.toJSONString(rsp));
      return null;
    }
    String replicaName = null;
    for (Object o : list) {
      Map<String, Object> map = (Map<String, Object>)o;
      String op = (String)map.get("DaemonOp");
      if (op == null) {
        continue;
      }
      String[] parts = op.split("\\s+");
      if (parts.length != 4) {
        log.debug("Invalid daemon location info, expected 4 tokens: " + op);
        return null;
      }
      // check if it's plausible
      if (parts[3].contains("shard") && parts[3].contains("replica")) {
        replicaName = parts[3];
        break;
      } else {
        log.debug("daemon location info likely invalid: " + op);
        return null;
      }
    }
    if (replicaName == null) {
      return null;
    }
    // build a baseUrl of the replica
    for (Replica r : coll.getReplicas()) {
      if (replicaName.equals(r.getCoreName())) {
        return r.getBaseUrl() + "/" + r.getCoreName();
      }
    }
    return null;
  }

  // XXX currently this is complicated to due a bug in the way the daemon 'list'
  // XXX operation is implemented - see SOLR-13245. We need to query the actual
  // XXX SolrCore where the daemon is running
  private void waitForDaemon(String daemonName, String daemonUrl, String collection) throws Exception {
    HttpClient client = ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrl(daemonUrl).build()) {
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      q.set("action", "list");
      q.set(CommonParams.DISTRIB, false);
      QueryRequest req = new QueryRequest(q);
      boolean isRunning;
      do {
        isRunning = false;
        try {
          NamedList<Object> rsp = solrClient.request(req);
          Map<String, Object> rs = (Map<String, Object>)rsp.get("result-set");
          if (rs == null || rs.isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing result-set: " + Utils.toJSONString(rsp));
          }
          List<Object> list = (List<Object>)rs.get("docs");
          if (list == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing result-set: " + Utils.toJSONString(rsp));
          }
          if (list.isEmpty()) { // finished?
            break;
          }
          for (Object o : list) {
            Map<String, Object> map = (Map<String, Object>)o;
            String id = (String)map.get("id");
            if (daemonName.equals(id)) {
              isRunning = true;
              break;
            }
          }
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception copying the documents", e);
        }
        ocmh.cloudManager.getTimeSource().sleep(5000);
      } while (isRunning && !maybeAbort(collection));
    }
  }

  private void killDaemon(String daemonName, String daemonUrl) throws Exception {
    HttpClient client = ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrl(daemonUrl).build()) {
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      q.set("action", "kill");
      q.set(CommonParams.ID, daemonName);
      q.set(CommonParams.DISTRIB, false);
      QueryRequest req = new QueryRequest(q);
      NamedList<Object> rsp = solrClient.request(req);
      // /result-set/docs/[0]/DaemonOp : Deamon:id killed on coreName

      // nocommit error checking
    }
  }

  private void cleanup(String collection, String targetCollection, String chkCollection, String daemonUrl, String daemonName) throws Exception {
    // 1. kill the daemon
    // 2. cleanup target / chk collections IFF the source collection still exists and is not empty
    // 3. cleanup collection state

    if (daemonUrl != null) {
      killDaemon(daemonName, daemonUrl);
    }
    ClusterState clusterState = ocmh.cloudManager.getClusterStateProvider().getClusterState();
    NamedList<Object> cmdResults = new NamedList<>();
    if (!collection.equals(targetCollection) && clusterState.hasCollection(targetCollection)) {
      ZkNodeProps cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
          CommonParams.NAME, targetCollection,
          CoreAdminParams.DELETE_METRICS_HISTORY, "true"
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
      // nocommit error checking
    }
    ZkNodeProps props = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
        ZkStateReader.COLLECTION_PROP, collection,
        REINDEXING_PROP, State.ABORTED.toLower(),
        ZkStateReader.READ_ONLY, "");
    ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
  }
}
