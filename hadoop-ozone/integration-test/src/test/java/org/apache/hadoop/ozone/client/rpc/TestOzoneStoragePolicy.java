/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_DEFAULT_STORAGE_POLICY_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_DEFAULT_STORAGE_POLICY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration test for end-to-end key creation with StoragePolicy.
 * Verifies that keys written with a specific StoragePolicy end up
 * on the correct storage tiers at SCM and Datanode levels.
 */
public class TestOzoneStoragePolicy {
  private OzoneClient ozClient = null;
  private ObjectStore store = null;
  private OzoneManager ozoneManager;
  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private ContainerManager containerManager;
  private PipelineManager pipelineManager;

  /**
   * Create a MiniOzoneCluster for testing.
   */
  void startCluster(OzoneConfiguration ozoneConf,
      List<List<StorageType>> storageTypeList,
      int datanodeCount, int dataVolumesCount) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(ozoneConf)
        .setNumDatanodes(datanodeCount)
        .setDatanodeFactory(
            UniformDatanodesFactory.newBuilder()
                .setNumDataVolumes(dataVolumesCount)
                .setDatanodeStorageTypes(storageTypeList)
                .build())
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = cluster.newClient();
    store = ozClient.getObjectStore();
    ozoneManager = cluster.getOzoneManager();
    containerManager = cluster.getStorageContainerManager()
        .getContainerManager();
    pipelineManager = cluster.getStorageContainerManager()
        .getPipelineManager();
  }

  @AfterEach
  void shutdownCluster() throws IOException {
    IOUtils.closeQuietly(ozClient);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static Stream<Arguments> replicaTypeAll() {
    return Stream.of(
        arguments("RATIS", "ONE", BucketLayout.OBJECT_STORE),
        arguments("RATIS", "THREE", BucketLayout.OBJECT_STORE)
    );
  }

  public static Stream<Arguments> replicaType() {
    return Stream.of(
        arguments("RATIS", "THREE", BucketLayout.OBJECT_STORE)
    );
  }

  ReplicationConfig getReplicationConfig(String type, String replication) {
    return ReplicationConfig.fromTypeAndFactor(
        ReplicationType.valueOf(type),
        ReplicationFactor.valueOf(replication));
  }

  @ParameterizedTest
  @MethodSource("replicaTypeAll")
  public void testStoragePolicy(
      String type, String replication,
      BucketLayout bucketLayout) throws Exception {
    ReplicationConfig replicationConfig =
        getReplicationConfig(type, replication);

    // Create a cluster with DNs, each DN has three different
    // StorageType volumes
    List<List<StorageType>> storageTypeList = new ArrayList<>();
    for (int i = 1; i <= replicationConfig.getRequiredNodes(); i++) {
      storageTypeList.add(Arrays.asList(
          StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE));
    }
    startCluster(conf, storageTypeList,
        replicationConfig.getRequiredNodes(), 3);
    List<OzoneStoragePolicy> storagePolicies =
        new ArrayList<>(Arrays.asList(OzoneStoragePolicy.values()));
    storagePolicies.add(null);

    for (OzoneStoragePolicy storagePolicy : storagePolicies) {
      OmKeyInfo keyInfo = createRandomNameKeyAndGet(
          replicationConfig, storagePolicy, bucketLayout);
      OzoneStoragePolicy expectedPolicy =
          getFinalStoragePolicy(storagePolicy);

      assertKeyInfo(keyInfo, 1, expectedPolicy, replicationConfig);
      assertSCMPipelineAndContainer(keyInfo,
          expectedPolicy.getCreationTier(), replicationConfig);
      assertDNContainer(keyInfo, expectedPolicy.getCreationTier(),
          replicationConfig);
    }
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  public void testBucketStoragePolicy(
      String type, String replication,
      BucketLayout bucketLayout) throws Exception {
    ReplicationConfig replicationConfig =
        getReplicationConfig(type, replication);

    // Create a cluster with DNs, each DN has three different
    // StorageType volumes
    List<List<StorageType>> storageTypeList = new ArrayList<>();
    for (int i = 1; i <= replicationConfig.getRequiredNodes(); i++) {
      storageTypeList.add(Arrays.asList(
          StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE));
    }
    startCluster(conf, storageTypeList,
        replicationConfig.getRequiredNodes(), 3);

    List<OzoneStoragePolicy> storagePolicies =
        new ArrayList<>(Arrays.asList(OzoneStoragePolicy.values()));
    for (OzoneStoragePolicy bucketStoragePolicy : storagePolicies) {
      // If no StoragePolicy is specified in keyArgs when writing a key,
      // then the key's StoragePolicy inherits the bucket StoragePolicy.
      OzoneBucket ozoneBucket = createBucketWithStoragePolicyAndGet(
          bucketStoragePolicy, replicationConfig, bucketLayout);
      assertEquals(bucketStoragePolicy, ozoneBucket.getStoragePolicy());
      OmKeyInfo keyInfo = createKeyWithStoragePolicyAndGet(
          ozoneBucket, null, null);
      assertKeyInfo(keyInfo, 1, bucketStoragePolicy, replicationConfig);

      // If a StoragePolicy is specified in keyArgs when writing a key,
      // the StoragePolicy of keyArgs takes precedence.
      int currentIndex = storagePolicies.indexOf(bucketStoragePolicy);
      OzoneStoragePolicy anotherStoragePolicy = storagePolicies.get(
          (currentIndex + 1) % storagePolicies.size());
      keyInfo = createKeyWithStoragePolicyAndGet(
          ozoneBucket, anotherStoragePolicy, null);
      assertKeyInfo(keyInfo, 1, anotherStoragePolicy, replicationConfig);
      assertSCMPipelineAndContainer(keyInfo,
          anotherStoragePolicy.getCreationTier(), replicationConfig);
      assertDNContainer(keyInfo, anotherStoragePolicy.getCreationTier(),
          replicationConfig);
    }
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  public void testWriteKeyMultipleTimesWithStoragePolicy(
      String type, String replication,
      BucketLayout bucketLayout) throws Exception {
    ReplicationConfig replicationConfig =
        getReplicationConfig(type, replication);
    int times = 5;

    List<List<StorageType>> storageTypeList = new ArrayList<>();
    for (int i = 1; i <= replicationConfig.getRequiredNodes(); i++) {
      storageTypeList.add(Arrays.asList(
          StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE));
    }
    startCluster(conf, storageTypeList,
        replicationConfig.getRequiredNodes(), 3);
    List<OzoneStoragePolicy> storagePolicies =
        new ArrayList<>(Arrays.asList(OzoneStoragePolicy.values()));

    for (OzoneStoragePolicy storagePolicy : storagePolicies) {
      OzoneBucket ozoneBucket = createBucketWithStoragePolicyAndGet(
          storagePolicy, replicationConfig, bucketLayout);
      assertEquals(storagePolicy, ozoneBucket.getStoragePolicy());
      for (int i = 0; i < times; i++) {
        OmKeyInfo keyInfo = createKeyWithStoragePolicyAndGet(
            ozoneBucket, storagePolicy, replicationConfig);
        assertKeyInfo(keyInfo, 1, storagePolicy, replicationConfig);
        assertSCMPipelineAndContainer(keyInfo,
            storagePolicy.getCreationTier(), replicationConfig);
        assertDNContainer(keyInfo, storagePolicy.getCreationTier(),
            replicationConfig);
      }
    }
  }

  private OzoneStoragePolicy getFinalStoragePolicy(
      OzoneStoragePolicy storagePolicy) {
    if (storagePolicy == null) {
      return OzoneStoragePolicy.valueOf(conf.get(
          OZONE_DEFAULT_STORAGE_POLICY_KEY,
          OZONE_DEFAULT_STORAGE_POLICY_DEFAULT));
    }
    return storagePolicy;
  }

  private void assertKeyInfo(OmKeyInfo keyInfo, int expectedBlockCount,
      OzoneStoragePolicy expectedPolicy,
      ReplicationConfig expectedReplication) {
    assertEquals(1, keyInfo.getKeyLocationVersions().size());
    assertNotNull(keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly());
    assertEquals(expectedBlockCount, keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly().size());
    assertEquals(expectedPolicy, keyInfo.getStoragePolicy());
    assertEquals(expectedReplication, keyInfo.getReplicationConfig());
  }

  private void assertSCMPipelineAndContainer(OmKeyInfo keyInfo,
      StorageTier expectedStorageTier,
      ReplicationConfig expectedReplication) throws Exception {
    OmKeyLocationInfo omKeyLocationInfo = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);
    long containerID = omKeyLocationInfo.getContainerID();
    ContainerInfo scmContainerInfo =
        containerManager.getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline =
        pipelineManager.getPipeline(scmContainerInfo.getPipelineID());
    // Assert SCM Container and Pipeline
    assertEquals(expectedReplication, pipeline.getReplicationConfig());
    assertEquals(expectedStorageTier, scmContainerInfo.getStorageTier(),
        "pipeline" + pipeline.getId());
    assertEquals(Pipeline.PipelineState.OPEN,
        pipeline.getPipelineState());
    assertEquals(expectedReplication.getRequiredNodes(),
        pipeline.getNodeSet().size());
    assertTrue(pipeline.getSupportedStorageTier()
        .contains(expectedStorageTier));
  }

  private void assertDNContainer(OmKeyInfo keyInfo,
      StorageTier expectedStorageTier,
      ReplicationConfig expectedReplication) throws Exception {
    StorageType expectedStorageType =
        StorageTierUtil.getStorageTypeForUniformStorageTier(
            expectedStorageTier);
    OmKeyLocationInfo omKeyLocationInfo = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);
    long containerID = omKeyLocationInfo.getContainerID();
    ContainerInfo scmContainerInfo =
        containerManager.getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline =
        pipelineManager.getPipeline(scmContainerInfo.getPipelineID());

    int keyNodeCount = 0;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      // Assert Datanode Container replica
      Container<?> datanodeContainer = hddsDatanode
          .getDatanodeStateMachine()
          .getContainer().getContainerSet()
          .getContainer(containerID);
      if (datanodeContainer == null) {
        continue;
      }
      ContainerData containerData = datanodeContainer.getContainerData();
      assertEquals(expectedStorageType, containerData.getStorageType());
      assertEquals(expectedStorageType,
          containerData.getVolume().getStorageType());
      assertTrue(pipeline.getNodeSet().contains(
          hddsDatanode.getDatanodeDetails()));
      keyNodeCount++;
    }
    assertEquals(expectedReplication.getRequiredNodes(), keyNodeCount);
  }

  private OzoneBucket createBucketWithStoragePolicyAndGet(
      StoragePolicy storagePolicy,
      ReplicationConfig replicationConfig,
      BucketLayout bucketLayout) throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setStoragePolicy(storagePolicy)
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(replicationConfig))
        .setBucketLayout(bucketLayout)
        .build();
    store.getVolume(volumeName).createBucket(bucketName, bucketArgs);
    return store.getVolume(volumeName).getBucket(bucketName);
  }

  private OmKeyInfo createKeyWithStoragePolicyAndGet(
      OzoneBucket ozoneBucket, StoragePolicy storagePolicy,
      ReplicationConfig replicationConfig) throws IOException {
    String keyValue = "value";
    String keyName = UUID.randomUUID().toString();
    OzoneOutputStream out = ozoneBucket.createKey(keyName,
        keyValue.getBytes(UTF_8).length, replicationConfig,
        new HashMap<>(), new HashMap<>(), storagePolicy);
    out.write(keyValue.getBytes(UTF_8));
    out.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setKeyName(keyName)
        .build();
    return ozoneManager.lookupKey(keyArgs);
  }

  private OmKeyInfo createRandomNameKeyAndGet(
      ReplicationConfig replicationConfig,
      StoragePolicy storagePolicy,
      BucketLayout bucketLayout) throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String keyValue = "value";
    store.createVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout)
        .build();
    store.getVolume(volumeName).createBucket(bucketName, bucketArgs);
    OzoneBucket bucket =
        store.getVolume(volumeName).getBucket(bucketName);

    OzoneOutputStream out = bucket.createKey(keyName,
        keyValue.getBytes(UTF_8).length, replicationConfig,
        new HashMap<>(), new HashMap<>(), storagePolicy);
    out.write(keyValue.getBytes(UTF_8));
    out.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    return ozoneManager.lookupKey(keyArgs);
  }
}
