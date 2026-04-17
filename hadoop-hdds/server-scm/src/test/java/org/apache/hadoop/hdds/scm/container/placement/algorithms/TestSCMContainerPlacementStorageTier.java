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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import static org.apache.hadoop.hdds.client.StorageTypeUtils.getStorageTypeProto;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests that the placement policy correctly filters nodes by StorageType
 * when choosing datanodes for pipelines/containers.
 */
class TestSCMContainerPlacementStorageTier {

  @Test
  public void chooseDatanodes() throws IOException {
    // Prepare Env
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);
    List<DatanodeInfo> datanodes = new ArrayList<>();
    DatanodeInfo archiveDn1 =
        createDatanodeWithStorageType(StorageType.ARCHIVE);
    DatanodeInfo diskDn1 = createDatanodeWithStorageType(StorageType.DISK);
    DatanodeInfo ssdDn1 = createDatanodeWithStorageType(StorageType.SSD);
    NodeManager mockNodeManager = mock(NodeManager.class);

    PlacementPolicy policy =
        new SCMContainerPlacementRandom(mockNodeManager, conf, null, true,
            mock(SCMContainerPlacementMetrics.class));

    // Create one Datanode for each StorageType
    datanodes.add(archiveDn1);
    datanodes.add(diskDn1);
    datanodes.add(ssdDn1);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenAnswer(invocation -> new ArrayList<>(datanodes));

    // Can choose one Datanode for each StorageType.
    assertPlacementResult(policy, StorageType.ARCHIVE, 1, archiveDn1);
    assertPlacementResult(policy, StorageType.DISK, 1, diskDn1);
    assertPlacementResult(policy, StorageType.SSD, 1, ssdDn1);

    // Cannot choose two Datanodes of a specific StorageType because
    // there is only one in the cluster of that type.
    SCMException ex = assertThrows(SCMException.class, () ->
        assertPlacementResult(policy, StorageType.ARCHIVE, 2),
        "Expected SCMException");
    assertEquals(FAILED_TO_FIND_SUITABLE_NODE, ex.getResult());
    ex = assertThrows(SCMException.class, () ->
        assertPlacementResult(policy, StorageType.DISK, 2),
        "Expected SCMException");
    assertEquals(FAILED_TO_FIND_SUITABLE_NODE, ex.getResult());
    ex = assertThrows(SCMException.class, () ->
        assertPlacementResult(policy, StorageType.SSD, 2),
        "Expected SCMException");
    assertEquals(FAILED_TO_FIND_SUITABLE_NODE, ex.getResult());

    // Add one Datanode for each StorageType then there are
    // two Datanodes of each StorageType.
    DatanodeInfo archiveDn2 =
        createDatanodeWithStorageType(StorageType.ARCHIVE);
    DatanodeInfo diskDn2 = createDatanodeWithStorageType(StorageType.DISK);
    DatanodeInfo ssdDn2 = createDatanodeWithStorageType(StorageType.SSD);
    datanodes.add(archiveDn2);
    datanodes.add(diskDn2);
    datanodes.add(ssdDn2);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenAnswer(invocation -> new ArrayList<>(datanodes));

    // Now can choose two Datanodes for each StorageType.
    assertPlacementResult(policy, StorageType.ARCHIVE, 2,
        archiveDn1, archiveDn2);
    assertPlacementResult(policy, StorageType.DISK, 2,
        diskDn1, diskDn2);
    assertPlacementResult(policy, StorageType.SSD, 2,
        ssdDn1, ssdDn2);
  }

  private void assertPlacementResult(PlacementPolicy policy,
      StorageType storageType, int numNodes,
      DatanodeInfo... expectedNodes) throws IOException {
    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(
        null, null, numNodes, 0, 1, storageType);
    assertEquals(numNodes, datanodeDetails.size());
    List<String> uuids = datanodeDetails.stream()
        .map(DatanodeDetails::getUuidString)
        .collect(Collectors.toList());
    for (DatanodeInfo expectedNode : expectedNodes) {
      assertTrue(uuids.contains(expectedNode.getUuidString()));
    }
  }

  private DatanodeInfo createDatanodeWithStorageType(
      StorageType storageType) {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto storage1 = HddsTestUtils.createStorageReport(
        datanodeInfo.getID(),
        "/data1-" + datanodeInfo.getUuidString(),
        100L, 0, 100L, getStorageTypeProto(storageType));
    MetadataStorageReportProto metaStorage1 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodeInfo.getUuidString(),
            100L, 0, 100L, getStorageTypeProto(storageType));
    datanodeInfo.updateStorageReports(
        new ArrayList<>(Arrays.asList(storage1)));
    datanodeInfo.updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage1)));
    return datanodeInfo;
  }
}
