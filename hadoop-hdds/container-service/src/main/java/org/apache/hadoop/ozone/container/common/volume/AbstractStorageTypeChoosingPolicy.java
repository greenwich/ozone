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

package org.apache.hadoop.ozone.container.common.volume;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;

/**
 * Abstract base class for implementing a VolumeChoosingPolicy that filters
 * volumes by a specific StorageType before applying a selection strategy.
 *
 * Subclasses must implement the chooseVolumeInternal method, which defines
 * the strategy for selecting a volume from the filtered list.
 */
public abstract class AbstractStorageTypeChoosingPolicy
    implements VolumeChoosingPolicy {

  private StorageType defaultStorageType;

  @Override
  public void init(StorageType storageType) {
    this.defaultStorageType = storageType;
  }

  /**
   * Choose a volume to place a container,
   * given a list of volumes and the max container size sought for storage.
   *
   * The implementations of this method must be thread-safe.
   *
   * @param volumes - a list of available volumes.
   * @param maxContainerSize - the maximum size of the container for which a
   *                         volume is sought.
   * @return the chosen volume.
   * @throws IOException when disks are unavailable or are full.
   */
  protected abstract HddsVolume chooseVolumeInternal(
      List<HddsVolume> volumes, long maxContainerSize) throws IOException;

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
      long maxContainerSize, StorageType storageType) throws IOException {
    final StorageType finalStorageType =
        (storageType == null) ? defaultStorageType : storageType;

    // Filter only when a storageType is explicitly provided
    List<HddsVolume> filteredVolumes = (storageType == null) ? volumes :
        volumes.stream()
            .filter(volume -> volume.getStorageType().equals(finalStorageType))
            .collect(Collectors.toList());

    return chooseVolumeInternal(filteredVolumes, maxContainerSize);
  }
}
