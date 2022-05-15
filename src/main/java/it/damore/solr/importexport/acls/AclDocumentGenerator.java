package it.damore.solr.importexport.acls;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * ACL document generator - utility for taking an ACL SolrInputDocument and generating a SolrInputDocument
 * for each shard in the collection.
 *
 * This way, the graph+join query for security trimming will not have to do any
 * cross-core or cross-collection communication. The id of each document will be computed
 * dynamically by using the input document's $aclIdFieldName + "___k" where we increase k until we have
 * an ID that spans each shard. So if you have 32 shards and $aclIdFieldName = "myid", you will have a variety of
 * id's such as "myid___1", "myid___5", "myid_92" etc.
 */
public class AclDocumentGenerator {
  private static final Logger logger = LoggerFactory.getLogger(AclDocumentGenerator.class);

  private DocRouter router;
  private DocCollection docCollection;
  private int numSlices;
  private boolean hashIdToLong = false;

  private static final int MAX_ATTEMPTS_TO_FIND_SHARD =
    Integer.parseInt(System.getProperty("com.lucidworks.solrClient.maxAttemptsToFindShard", "5000"));

  public static final RetryPolicy RETRY_POLICY = new RetryPolicy()
    .withMaxRetries(Integer.parseInt(System.getProperty("com.lucidworks.solrClient.retry", "5")))
    .retryOn(Arrays.asList(SolrException.class, SolrServerException.class, TimeoutException.class));

  public AclDocumentGenerator(DocRouter router, DocCollection docCollection, int numSlices, boolean hashIdToLong) {
    this.router = router;
    this.docCollection = docCollection;
    this.numSlices = numSlices;
    this.hashIdToLong = hashIdToLong;
  }

  public AclDocumentGenerator(SolrClient solrClient, String collectionName) {
    CloudSolrClient cloudSolrClient = (CloudSolrClient) solrClient;
    docCollection = Failsafe
      .with(RETRY_POLICY)
      .get(() -> ZkStateReader.getCollectionLive(cloudSolrClient.getZkStateReader(), collectionName));
    router = docCollection.getRouter();
    numSlices = docCollection.getSlices().size();
  }

  public AclDocumentGenerator(CloudSolrClient client, String collectionName, boolean hashIdToLong) {
    docCollection = Failsafe
      .with(RETRY_POLICY)
      .get(() -> ZkStateReader.getCollectionLive(client.getZkStateReader(), collectionName));
    router = docCollection.getRouter();
    numSlices = docCollection.getSlices().size();
    this.hashIdToLong = hashIdToLong;
  }

  /**
   * This will take a single SolrInputDocument and generate a list of SolrInputDocument's where we guarantee
   * the input document is on each shard.
   * @param doc The document we want to insert. The "id" of the document will be appended to in order to make a
   *            unique id. ___1, ___2 etc.
   * @param aclIdFieldName The field name that contains the ID of the acl.
   * @return Returns a list of acl documents.
   */
  public List<SolrInputDocument> createAclDocuments(SolrInputDocument doc, String aclIdFieldName) {
    List<SolrInputDocument> result = Lists.newArrayList();
    Map<String, String> assignedSlices = Maps.newHashMap();
    String id = String.valueOf(doc.getFieldValue(aclIdFieldName));
    if (Strings.isNullOrEmpty(id)) {
      throw new AclGeneratorException(String.format("Null or empty aclFieldName on acl doc %s", doc));
    }
    if (hashIdToLong) {
      assignSlicesWithUUID5GeneratorHash(doc, assignedSlices, id);
    } else {
      assignSlicesUsingIdAsHash(doc, assignedSlices, id);
    }
    for (Map.Entry<String, String> assignedSlice : assignedSlices.entrySet()) {
      SolrInputDocument nextDoc = doc.deepCopy();
      nextDoc.setField("id", assignedSlice.getValue());
      nextDoc.setField("_lw_acl_doc_b", true);
      nextDoc.setField("shard_s", assignedSlice.getKey());
      result.add(nextDoc);
    }
    return result;
  }

  /**
   * Assign ids of slices as uuid5HashLong(id) + _ + incrementingIndex until you find a match.
   */
  private void assignSlicesWithUUID5GeneratorHash(SolrInputDocument doc, Map<String, String> assignedSlices, String id) {
    int idx = 0;
    int maxAttempts = MAX_ATTEMPTS_TO_FIND_SHARD;
    long longEquivOfId = UUID5Generator.toLongUUID5(id);
    while (assignedSlices.size() < numSlices && --maxAttempts > 0) {
      // keep going until we have a doc for each shard.
      String nextIdWithPostfix = String.format("%d___%d", longEquivOfId, idx++);
      Slice slice = router.getTargetSlice(nextIdWithPostfix, doc, null, null, docCollection);
      assignedSlices.putIfAbsent(slice.getName(), nextIdWithPostfix);
    }
    if (maxAttempts <= 0) {
      throw new AclGeneratorException(String.format("Could not generate a shard ID %s after %d attempts - numSlices=%d, " +
        "assignedSlices=%s", id, MAX_ATTEMPTS_TO_FIND_SHARD, numSlices, assignedSlices));
    }
  }

  /**
   * Assign ids of slices as id + _ + incrementingIndex until you find a match.
   * The id must have any ! replaced with _bang_ to avoid collisions because of solr issue with distributing ids with
   * ! in them.
   */
  private void assignSlicesUsingIdAsHash(SolrInputDocument doc, Map<String, String> assignedSlices, String id) {
    int idx = 0;
    int maxAttempts = MAX_ATTEMPTS_TO_FIND_SHARD;
    String idToUseForHash = StringUtils.replace(id, "!", "_BNG_");
    while (assignedSlices.size() < numSlices && --maxAttempts > 0) {
      // keep going until we have a doc for each shard.
      String nextIdWithPostfix = String.format("%s___%d", idToUseForHash, idx++);
      Slice slice = router.getTargetSlice(nextIdWithPostfix, doc, null, null, docCollection);
      assignedSlices.putIfAbsent(slice.getName(), nextIdWithPostfix);
    }
    if (maxAttempts <= 0) {
      throw new AclGeneratorException(String.format("Could not generate a shard ID %s after %d attempts - numSlices=%d, " +
        "assignedSlices=%s", id, MAX_ATTEMPTS_TO_FIND_SHARD, numSlices, assignedSlices));
    }
  }

  public boolean isHashIdToLong() {
    return hashIdToLong;
  }

  public AclDocumentGenerator setHashIdToLong(boolean hashIdToLong) {
    this.hashIdToLong = hashIdToLong;
    return this;
  }

  public int getNumSlices() {
    return numSlices;
  }
}

