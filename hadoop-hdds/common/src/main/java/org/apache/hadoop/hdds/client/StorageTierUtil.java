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

package org.apache.hadoop.hdds.client;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class for managing StorageTier operations.
 */
public final class StorageTierUtil {
  private StorageTierUtil() {
  }

  public static void validateNotEmpty(StorageTier storageTier) throws SCMException {
    if (storageTier.equals(StorageTier.EMPTY)) {
      throw new SCMException("Cannot create Pipeline for empty tier",
          SCMException.ResultCodes.CANNOT_CREATE_PIPELINE_FOR_EMPTY_TIER);
    }
  }

  public static StorageType getStorageTypeForUniformStorageTier(StorageTier storageTier, ReplicationConfig config)
      throws SCMException {
    validateNotEmpty(storageTier);
    List<StorageType> storageTypes = storageTier.getStorageTypes(config);
    if (storageTier.isUniformStorageType()) {
      return storageTypes.get(0);
    } else {
      throw new SCMException("Unsupported non-uniform storage tier " + storageTier,
          SCMException.ResultCodes.UNSUPPORTED_NON_UNIFORM_STORAGE_TIER);
    }
  }

  /**
   * Holds information about a storage type and the count of replicas
   * that should be placed on volumes of that type.
   */
  public static class StorageTypeInfo {
    private final StorageType storageType;
    private final int count;

    public StorageTypeInfo(StorageType storageType, int count) {
      this.storageType = storageType;
      this.count = count;
    }

    public StorageType getStorageType() {
      return storageType;
    }

    public int getCount() {
      return count;
    }
  }

  public static List<StorageTypeInfo> getStorageTypeInfoList(StorageTier storageTier, ReplicationConfig config)
      throws SCMException {
    validateNotEmpty(storageTier);
    List<StorageType> storageTypes = storageTier.getStorageTypes(config);
    List<StorageTypeInfo> storageTypeInfoList = new ArrayList<>();
    Set<StorageType> seen = Sets.newHashSet();
    for (StorageType type : storageTypes) {
      if (seen.add(type)) {
        int count = (int) storageTypes.stream().filter(t -> t == type).count();
        storageTypeInfoList.add(new StorageTypeInfo(type, count));
      }
    }
    return storageTypeInfoList;
  }
}
