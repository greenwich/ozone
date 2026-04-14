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

package org.apache.hadoop.ozone.shell.bucket;

import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Executes update bucket calls.
 */
@Command(name = "update",
    description = "Updates the parameters of the bucket")
public class UpdateBucketHandler extends BucketHandler {

  @Option(names = {"--user", "-u"},
      description = "Owner of the bucket to set")
  private String ownerName;

  @CommandLine.Option(names = {"--storagepolicy", "-sp"},
      description = "Allowed Bucket Storage Policy String values: HOT, WARM, COLD or null")
  private String storagePolicyStr;

  @CommandLine.Option(names = {"--allowFallBackStoragePolicy", "-asp"},
      description = "If true, allows writing to the backing storage tier during creation")
  private String allowFallBackStoragePolicyStr;

  private static final String NULL_STORAGE_POLICY = "null";

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneBucket bucket = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName);

    if (ownerName != null && !ownerName.isEmpty()) {
      boolean result = bucket.setOwner(ownerName);
      if (LOG.isDebugEnabled() && !result) {
        out().format("Bucket '%s' owner is already '%s'. Unchanged.%n",
            volumeName + "/" + bucketName, ownerName);
      }
    }

    // Update StoragePolicy
    if (!Strings.isNullOrEmpty(storagePolicyStr) ||
        !Strings.isNullOrEmpty(allowFallBackStoragePolicyStr)) {
      OmBucketArgs.Builder bucketArgsBuilder = OmBucketArgs.newBuilder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName);

      if (!Strings.isNullOrEmpty(storagePolicyStr)) {
        if (NULL_STORAGE_POLICY.equalsIgnoreCase(storagePolicyStr)) {
          bucketArgsBuilder.setUnSetStoragePolicy(true);
        } else {
          try {
            StoragePolicy storagePolicy =
                OzoneStoragePolicy.valueOf(storagePolicyStr.toUpperCase());
            bucketArgsBuilder.setStoragePolicy(storagePolicy);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid storage policy: " +
                storagePolicyStr +
                ". Allowed String values are: HOT, WARM, COLD, or null.");
          }
        }
      }
      if (!Strings.isNullOrEmpty(allowFallBackStoragePolicyStr)) {
        bucketArgsBuilder.setAllowFallbackStoragePolicy(
            Boolean.valueOf(allowFallBackStoragePolicyStr));
      }
      bucket.setStorageStoragePolicyProperty(bucketArgsBuilder.build());
    }

    OzoneBucket updatedBucket = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName);
    printObjectAsJson(updatedBucket);
  }
}
