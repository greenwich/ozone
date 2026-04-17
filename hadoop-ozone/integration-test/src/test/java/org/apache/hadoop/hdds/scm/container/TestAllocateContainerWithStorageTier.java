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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.junit.jupiter.api.Test;

/**
 * Integration test for container allocation with StorageTier.
 * Tests that containers are allocated on pipelines matching
 * the requested StorageTier.
 */
public class TestAllocateContainerWithStorageTier {

  private MiniOzoneCluster cluster;
  private ContainerManager containerManager;

  private final ReplicationConfig ratisThree =
      RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private final ReplicationConfig standaloneOne =
      StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE);

  public void createCluster(
      List<List<StorageType>> storageTypeList) throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    int numDatanodes = storageTypeList.size();
    int numVolumes = storageTypeList.get(0).size();

    cluster = MiniOzoneCluster
        .newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .setDatanodeFactory(
            UniformDatanodesFactory.newBuilder()
                .setNumDataVolumes(numVolumes)
                .setDatanodeStorageTypes(storageTypeList)
                .build())
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    StorageContainerManager scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
  }

  public void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAllocateContainerWithStorageTierCase1() throws Exception {
    // Each datanode has all three storage types
    createCluster(Arrays.asList(
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD,
            StorageType.ARCHIVE)));

    try {
      PipelineManager pipelineManager =
          cluster.getStorageContainerManager().getPipelineManager();
      assertTrue(containerManager.getContainers().isEmpty());
      ContainerInfo container;
      List<Pipeline> pipelines;

      // Since all datanodes have DISK, SSD, ARCHIVE volumes,
      // allocateContainer should succeed on any StorageTier
      container = containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager,
          ratisThree, StorageTier.DISK, 1);

      container = containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.SSD);
      assertContainer(container.containerID(), containerManager,
          ratisThree, StorageTier.SSD, 2);

      container = containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.ARCHIVE);
      assertContainer(container.containerID(), containerManager,
          ratisThree, StorageTier.ARCHIVE, 3);

      container = containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager,
          standaloneOne, StorageTier.DISK, 4);

      container = containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.SSD);
      assertContainer(container.containerID(), containerManager,
          standaloneOne, StorageTier.SSD, 5);

      container = containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.ARCHIVE);
      assertContainer(container.containerID(), containerManager,
          standaloneOne, StorageTier.ARCHIVE, 6);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.DISK);
      assertPipeline(pipelines, 1, StorageTier.DISK);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.SSD);
      assertPipeline(pipelines, 1, StorageTier.SSD);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.ARCHIVE);
      assertPipeline(pipelines, 1, StorageTier.ARCHIVE);

    } finally {
      cleanUp();
    }
  }

  @Test
  public void testAllocateContainerWithStorageTierCase2() throws Exception {
    // Not all datanodes have SSD and ARCHIVE
    createCluster(Arrays.asList(
        Arrays.asList(StorageType.DISK, StorageType.DISK,
            StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD,
            StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD,
            StorageType.SSD)));
    try {
      PipelineManager pipelineManager =
          cluster.getStorageContainerManager().getPipelineManager();
      assertTrue(containerManager.getContainers().isEmpty());
      ContainerInfo container;
      List<Pipeline> pipelines;

      // Since all datanodes have DISK but not all have SSD/ARCHIVE,
      // Ratis THREE allocation should only succeed for DISK
      assertThrows(IOException.class, () ->
          containerManager.allocateContainer(
              ratisThree, "admin", StorageTier.SSD));

      container = containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager,
          ratisThree, StorageTier.DISK, 1);

      assertThrows(IOException.class, () ->
          containerManager.allocateContainer(
              ratisThree, "admin", StorageTier.ARCHIVE));

      // Standalone ONE can use any single node
      container = containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager,
          standaloneOne, StorageTier.DISK, 2);

      container = containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.ARCHIVE);
      assertContainer(container.containerID(), containerManager,
          standaloneOne, StorageTier.ARCHIVE, 3);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.DISK);
      assertPipeline(pipelines, 1, StorageTier.DISK);

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.SSD).isEmpty());

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.ARCHIVE).isEmpty());
    } finally {
      cleanUp();
    }
  }

  @Test
  public void testAllocateContainerWithStorageTierCase3() throws Exception {
    // All datanodes only have DISK volumes
    createCluster(Arrays.asList(
        Arrays.asList(StorageType.DISK, StorageType.DISK, StorageType.DISK),
        Arrays.asList(StorageType.DISK, StorageType.DISK, StorageType.DISK),
        Arrays.asList(StorageType.DISK, StorageType.DISK,
            StorageType.DISK)));
    try {
      PipelineManager pipelineManager =
          cluster.getStorageContainerManager().getPipelineManager();
      assertTrue(containerManager.getContainers().isEmpty());
      ContainerInfo container;
      List<Pipeline> pipelines;

      // Only DISK volumes, so only DISK allocation should succeed
      assertThrows(IOException.class, () ->
          containerManager.allocateContainer(
              ratisThree, "admin", StorageTier.SSD));

      container = containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager,
          ratisThree, StorageTier.DISK, 1);

      assertThrows(IOException.class, () ->
          containerManager.allocateContainer(
              ratisThree, "admin", StorageTier.ARCHIVE));

      assertThrows(IOException.class, () ->
          containerManager.allocateContainer(
              standaloneOne, "admin", StorageTier.SSD));

      container = containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager,
          standaloneOne, StorageTier.DISK, 2);

      assertThrows(IOException.class, () ->
          containerManager.allocateContainer(
              standaloneOne, "admin", StorageTier.ARCHIVE));

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.DISK);
      assertPipeline(pipelines, 1, StorageTier.DISK);

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.SSD).isEmpty());

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, StorageTier.ARCHIVE).isEmpty());

    } finally {
      cleanUp();
    }
  }

  private void assertContainer(ContainerID containerID,
      ContainerManager manager, ReplicationConfig replicationConfig,
      StorageTier expectedStorageTier, int expectedTotalCount)
      throws IOException {
    ContainerInfo containerInfo = manager.getContainer(containerID);
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(containerInfo.getPipelineID());

    assertNotNull(containerInfo);
    assertEquals(expectedTotalCount, manager.getContainers().size());
    assertEquals(replicationConfig.getRequiredNodes(),
        pipeline.getNodes().size());
    assertTrue(pipeline.getSupportedStorageTier()
        .contains(expectedStorageTier));
    assertEquals(expectedStorageTier, containerInfo.getStorageTier());
  }

  private void assertPipeline(List<Pipeline> pipelines,
      int expectedContainerCount, StorageTier expectedStorageTier) {
    assertFalse(pipelines.isEmpty());
    List<ContainerInfo> containerInfos = new ArrayList<>();
    for (Pipeline pipeline : pipelines) {
      ContainerInfo containerInfo = containerManager.getMatchingContainer(
          0, "admin", pipeline, new HashSet<>(), expectedStorageTier);
      if (containerInfo != null) {
        containerInfos.add(containerInfo);
      }
    }
    assertEquals(expectedContainerCount, containerInfos.size());
    for (ContainerInfo containerInfo : containerInfos) {
      assertEquals(expectedStorageTier, containerInfo.getStorageTier());
    }
  }
}
