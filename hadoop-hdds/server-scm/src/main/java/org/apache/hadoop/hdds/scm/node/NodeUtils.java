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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for Node operations related to storage tiers.
 */
public final class NodeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(NodeUtils.class);

  private NodeUtils() {
  }

  /**
   * Determines the supported StorageTiers for a pipeline based on
   * the storage types available across its member datanodes.
   *
   * @param dns list of datanodes in the pipeline
   * @param nodeManager the node manager to look up datanode info
   * @return list of supported StorageTiers
   */
  /**
   * Returns the set of StorageTypes available on a single datanode.
   */
  public static Set<StorageType> getDatanodesStorageTypes(
      DatanodeDetails dn, NodeManager nodeManager) {
    Set<StorageType> storageTypes = new HashSet<>();
    DatanodeInfo datanodeInfo = nodeManager.getDatanodeInfo(dn);
    if (datanodeInfo == null) {
      throw new IllegalStateException("Cannot get Datanode : " + dn.getUuidString() + " Info");
    }
    List<StorageReportProto> storageReportProtos = datanodeInfo.getStorageReports();
    for (StorageReportProto storageReportProto : storageReportProtos) {
      storageTypes.add(getStorageTypeFromStorageReportProto(storageReportProto, datanodeInfo));
    }
    return storageTypes;
  }

  public static List<StorageTier> getDatanodesStorageTypes(
      List<DatanodeDetails> dns, NodeManager nodeManager) {
    List<Set<StorageType>> dnStorageTypes = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      Set<StorageType> uniqueStorageTypes = new HashSet<>();
      DatanodeInfo datanodeInfo = nodeManager.getDatanodeInfo(dn);
      if (datanodeInfo == null) {
        // Node not registered in NodeManager (e.g. read-only pipeline);
        // default to DISK
        dnStorageTypes.add(Collections.singleton(StorageType.DISK));
        continue;
      }
      List<StorageReportProto> storageReportProtos = datanodeInfo.getStorageReports();
      for (StorageReportProto storageReportProto : storageReportProtos) {
        uniqueStorageTypes.add(getStorageTypeFromStorageReportProto(storageReportProto, dn));
      }
      dnStorageTypes.add(uniqueStorageTypes);
    }
    return StorageTierUtil.findSupportedStorageTiers(dnStorageTypes);
  }

  /**
   * Computes a unique ID for the set of StorageTypes reported by a datanode.
   *
   * @param storageReportProtos storage reports from the datanode
   * @param datanodeDetails the datanode
   * @return computed storage types ID
   */
  public static long computeStorageTypesID(List<StorageReportProto> storageReportProtos,
      DatanodeDetails datanodeDetails) {
    Set<StorageType> storageTypes = new HashSet<>();
    for (StorageReportProto storageReportProto : storageReportProtos) {
      storageTypes.add(getStorageTypeFromStorageReportProto(storageReportProto, datanodeDetails));
    }
    return StorageTier.computeId(storageTypes);
  }

  /**
   * Extracts the StorageType from a StorageReportProto, falling back to DISK
   * if the type cannot be determined.
   *
   * @param storageReportProto the storage report
   * @param dn the datanode details (for logging)
   * @return the StorageType
   */
  public static StorageType getStorageTypeFromStorageReportProto(
      StorageReportProto storageReportProto, DatanodeDetails dn) {
    if (storageReportProto.hasStorageType()) {
      return StorageTypeUtils.getFromProtobuf(storageReportProto.getStorageType());
    } else {
      LOG.error("Cannot get Volume StorageType from datanode {},"
          + " falling back to DISK StorageType", dn.getUuidString());
      return StorageType.DISK;
    }
  }
}
