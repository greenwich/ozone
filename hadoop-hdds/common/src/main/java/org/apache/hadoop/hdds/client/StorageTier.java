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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTierProto;

/**
 * Ozone specific storage tiers.
 */
public enum StorageTier {
  SSD("SSD", StorageType.SSD),
  DISK("DISK", StorageType.DISK),
  ARCHIVE("ARCHIVE", StorageType.ARCHIVE),
  EMPTY("EMPTY");

  private final String tierName;
  private final List<StorageType> storageTypes;
  private final boolean uniformStorageType;
  private static final Map<StorageTier, Map<Integer, List<StorageType>>>
      CACHE = new EnumMap<>(StorageTier.class);
  private static final int MAX_NODE_COUNT = 20;

  /**
   * Maps each StorageType to a unique prime number. By multiplying these primes,
   * we generate a unique product representing a StorageTier ID.
   * When the number of nodes is the same, we can use this StorageTier ID
   * to uniquely identify a StorageTier.
   */
  private static final Map<StorageType, Integer> STORAGE_TYPE_PRIME_MAP = new HashMap<>();

  /**
   * Map&lt;node count, Map&lt;StorageTier ID, StorageTier&gt;&gt;.
   * When the number of nodes is the same, we can use this StorageTier ID
   * to uniquely identify a StorageTier.
   */
  private static final Map<Integer, Map<Long, StorageTier>>
      NODE_COUNT_TO_STORAGE_TIER_MAP = new HashMap<>();

  private static StorageTier defaultTier = DISK;

  StorageTier(String tierName) {
    this.tierName = tierName;
    this.storageTypes = Collections.emptyList();
    this.uniformStorageType = true;
  }

  // Constructor for uniform storage tiers
  StorageTier(String tierName, StorageType uniformStorageType) {
    this.tierName = tierName;
    this.storageTypes = Collections.singletonList(uniformStorageType);
    this.uniformStorageType = true;
  }

  // Constructor for non-uniform storage tiers
  StorageTier(String tierName, StorageType... storageTypes) {
    this.tierName = tierName;
    if (Arrays.stream(storageTypes).distinct().count() <= 1) {
      throw new IllegalArgumentException("StorageTier '" + tierName +
          "' requires at least two different StorageType instances." +
          " but only " + Arrays.stream(storageTypes).distinct().count() +
          " StorageType were provided.");
    }
    this.storageTypes = Arrays.asList(storageTypes);
    this.uniformStorageType = false;
  }

  static {
    // Assign unique primes to each StorageType
    int[] primes = {2, 3, 5, 7, 11, 13, 17, 19};
    int idx = 0;
    for (StorageType value : StorageType.values()) {
      STORAGE_TYPE_PRIME_MAP.put(value, primes[idx++]);
    }

    // Precompute storage type mappings for each node count
    for (StorageTier tier : StorageTier.values()) {
      Map<Integer, List<StorageType>> tierCache = new HashMap<>();
      for (int nodeCount = 0; nodeCount <= MAX_NODE_COUNT; nodeCount++) {
        List<StorageType> types = tier.computeStorageTypes(nodeCount);
        tierCache.put(nodeCount, types);
        long id = computeId(types);
        NODE_COUNT_TO_STORAGE_TIER_MAP
            .computeIfAbsent(nodeCount, k -> new HashMap<>())
            .put(id, tier);
      }
      CACHE.put(tier, tierCache);
    }
  }

  public StorageTierProto toProto() {
    switch (this) {
    case SSD:
      return StorageTierProto.SSD_TIER;
    case DISK:
      return StorageTierProto.DISK_TIER;
    case ARCHIVE:
      return StorageTierProto.ARCHIVE_TIER;
    default:
      throw new IllegalStateException(
          "Illegal StorageTier: " + this);
    }
  }

  public static StorageTier fromProto(StorageTierProto tier) {
    switch (tier) {
    case SSD_TIER:
      return SSD;
    case DISK_TIER:
      return DISK;
    case ARCHIVE_TIER:
      return ARCHIVE;
    default:
      throw new IllegalStateException(
          "Illegal StorageTierProto: " + tier);
    }
  }

  public String getTierName() {
    return tierName;
  }

  public boolean isUniformStorageType() {
    return uniformStorageType;
  }

  /**
   * Computes the list of StorageTypes based on node count.
   *
   * @param nodeCount The number of nodes.
   * @return The list of StorageTypes for the given tier and node count.
   */
  private List<StorageType> computeStorageTypes(int nodeCount) {
    if (isUniformStorageType()) {
      if (storageTypes.isEmpty()) {
        return Collections.emptyList();
      }
      return Collections.nCopies(nodeCount, storageTypes.get(0));
    } else {
      throw new UnsupportedOperationException(
          "Unsupported not uniform StorageTier: " + this);
    }
  }

  /**
   * Maps a StorageTier to its corresponding StorageType list based on node count.
   *
   * @param nodeCount The number of nodes.
   * @return The list of StorageTypes corresponding to the given tier and node count.
   * @throws IllegalArgumentException if the node count is not supported.
   */
  public List<StorageType> getStorageTypes(int nodeCount) {
    Map<Integer, List<StorageType>> tierCache = CACHE.get(this);

    if (tierCache != null) {
      List<StorageType> cachedStorageType = tierCache.get(nodeCount);
      if (cachedStorageType != null) {
        return cachedStorageType;
      }
    }

    throw new IllegalArgumentException("Unsupported node count: " +
        nodeCount + " for StorageTier: " + getTierName());
  }

  /**
   * Calculates a unique ID for a collection of StorageTypes by multiplying
   * their associated prime numbers.
   *
   * @param types the StorageType collection to calculate the ID for
   * @return the computed ID
   */
  public static long computeId(Collection<StorageType> types) {
    long computedId = 1;
    for (StorageType type : types) {
      long prime = STORAGE_TYPE_PRIME_MAP.get(type);
      if (computedId > Long.MAX_VALUE / prime) {
        throw new ArithmeticException("Overflow detected when calculating ID for StorageType.");
      }
      computedId *= prime;
    }
    return computedId;
  }

  /**
   * Returns the StorageTier corresponding to the given node count and computed ID.
   *
   * @param nodeCount number of nodes
   * @param id the computed StorageTier ID
   * @return the matching StorageTier, or null if not found
   */
  public static StorageTier fromID(int nodeCount, long id) {
    if (nodeCount > MAX_NODE_COUNT) {
      throw new IllegalArgumentException("Not supported node count: " + nodeCount
          + " Max supported node count: " + MAX_NODE_COUNT);
    }
    Map<Long, StorageTier> map = NODE_COUNT_TO_STORAGE_TIER_MAP.get(nodeCount);
    if (map != null) {
      return map.get(id);
    }
    return null;
  }

  /**
   * Returns the default StorageTier.
   */
  public static StorageTier getDefaultTier() {
    return defaultTier;
  }

  /**
   * Sets the default StorageTier.
   */
  public static void setDefault(StorageTier storageTier) {
    defaultTier = storageTier;
  }

}
