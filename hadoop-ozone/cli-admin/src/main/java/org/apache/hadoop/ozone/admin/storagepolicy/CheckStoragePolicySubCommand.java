/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.admin.storagepolicy;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import org.apache.hadoop.ozone.shell.keys.KeyUri;
import picocli.CommandLine;

/**
 * Check Key storage policy command.
 */
@CommandLine.Command(
    name = "check",
    description = "Check Key Storage Policy",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CheckStoragePolicySubCommand extends ScmSubcommand {

  @CommandLine.ParentCommand
  private StoragePolicyCommands parent;

  @CommandLine.Parameters(index = "0", arity = "1..1",
      description = "URI of the key.\n" + Shell.OZONE_URI_DESCRIPTION,
      converter = KeyUri.class)
  private OzoneAddress keyAddress;

  @CommandLine.Option(
      names = {"--show-replicas"},
      description = "Show detailed container replica information."
  )
  private boolean showReplicas;

  private boolean keyMatchStoragePolicy = true;

  @Override
  protected void execute(ScmClient scmClient) throws IOException {
    OzoneConfiguration conf = parent.getParent().getOzoneConf();
    try (OzoneClient ozoneClient = keyAddress.createClient(conf)) {
      OzoneVolume volume = ozoneClient.getObjectStore().getVolume(
          keyAddress.getVolumeName());
      OzoneBucket bucket = volume.getBucket(keyAddress.getBucketName());
      OzoneKeyDetails keyDetails = bucket.getKey(keyAddress.getKeyName());
      System.out.println(buildKeyStoragePolicyInfo(keyDetails));
      if (showReplicas) {
        System.out.println(
            buildContainerReplicaInfo(keyDetails, scmClient));
      }
    } catch (OzoneClientException e) {
      throw new IOException(e);
    }
  }

  private String buildKeyStoragePolicyInfo(OzoneKeyDetails keyInfo) {
    StringBuilder sb = new StringBuilder();
    StoragePolicy storagePolicy = keyInfo.getStoragePolicy();
    int requiredNodes = keyInfo.getReplicationConfig().getRequiredNodes();
    sb.append(String.format("Storage Policy for key '%s/%s/%s':%n",
        keyAddress.getVolumeName(), keyAddress.getBucketName(),
        keyAddress.getKeyName()));

    if (storagePolicy == null) {
      sb.append("  Key has no StoragePolicy set\n");
    } else {
      sb.append(String.format("  %s (Creation Tier: %s x %d)%n",
          storagePolicy, storagePolicy.getCreationTier(),
          requiredNodes));
      sb.append(String.format("  Key Match Storage Policy: %s%n",
          keyMatchStoragePolicy ? "YES" : "NO"));
    }
    return sb.toString();
  }

  private String buildContainerReplicaInfo(OzoneKeyDetails keyDetails,
      ScmClient scmClient) {
    StringBuilder sb = new StringBuilder();
    List<OzoneKeyLocation> keyLocations =
        keyDetails.getOzoneKeyLocations();
    StoragePolicy storagePolicy = keyDetails.getStoragePolicy();
    boolean hasStoragePolicy = storagePolicy != null;

    for (OzoneKeyLocation keyLocation : keyLocations) {
      long containerId = keyLocation.getContainerID();
      sb.append(String.format("Container ID: %d%n", containerId));
      boolean containerMatchesPolicy = true;

      try {
        List<ContainerReplicaInfo> replicas =
            scmClient.getContainerReplicas(containerId);
        for (ContainerReplicaInfo replica : replicas) {
          if (hasStoragePolicy
              && !isReplicaMatchingPolicy(replica, storagePolicy)) {
            containerMatchesPolicy = false;
            keyMatchStoragePolicy = false;
          }
          DatanodeDetails datanode = replica.getDatanodeDetails();
          sb.append(String.format(
              "  Datanode: %s (%s, %s)%n",
              datanode.getUuid(), datanode.getHostName(),
              datanode.getNetworkLocation()));
          sb.append(String.format("  Replica Storage Type: %s%n",
              replica.getStorageType()));
          sb.append(String.format("  Container Path: [%s]%s%n",
              replica.getVolumeStorageType(),
              replica.getContainerPath()));
          sb.append("\n");
        }
      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "Error: Unable to retrieve container details for %d",
            containerId), e);
      }

      String matchStatus = hasStoragePolicy
          ? (containerMatchesPolicy ? "YES" : "NO")
          : "Key has no StoragePolicy set";
      sb.append(String.format(
          "  Container Match Storage Policy: %s%n", matchStatus));
    }

    return sb.toString();
  }

  private boolean isReplicaMatchingPolicy(ContainerReplicaInfo replica,
      StoragePolicy storagePolicy) {
    try {
      StorageType expectedType =
          StorageTierUtil.getStorageTypeForUniformStorageTier(
              storagePolicy.getCreationTier());
      return replica.getStorageType() == expectedType
          && replica.getVolumeStorageType() == expectedType;
    } catch (IOException e) {
      return false;
    }
  }
}
