/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Client;
import alluxio.client.block.options.RemoveWorkerOptions;
import alluxio.master.MasterClientConfig;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a block master.
 */
@ThreadSafe
public interface BlockMasterClient extends Client {

  /**
   * Factory for {@link BlockMasterClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link BlockMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link BlockMasterClient} instance
     */
    public static BlockMasterClient create(MasterClientConfig conf) {
      return new RetryHandlingBlockMasterClient(conf);
    }
  }

  /**
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   */
  List<WorkerInfo> getWorkerInfoList() throws IOException;

  /**
   * Returns the {@link BlockInfo} for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the {@link BlockInfo}
   */
  BlockInfo getBlockInfo(final long blockId) throws IOException;

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   */
  long getCapacityBytes() throws IOException;

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   */
  long getUsedBytes() throws IOException;

  /**
   * Remove Worker from the cluster.
   *
   * @param address The address of the worker to remove
   * @param options The remove worker options
   */
  void removeWorker(final WorkerNetAddress address, final RemoveWorkerOptions options) throws IOException;

  /**
   * Delete Workers from the cluster.
   *
   * @param hosts The list of the delete worker host
   */
  void deleteWorker(final List<String> hosts) throws IOException;

  /**
   * Validate whether the worker of this address can be used as block store, and if it could,
   * reserve block size space on the worker. If not, return the latest information of all workers
   * 
   * @param address the address that need to be validated
   * @param preReserveBytes the size that need to be pre reserved
   * @return null if the address is available; all workers info otherwise
   */
  List<WorkerInfo> validateAndReserve(final WorkerNetAddress address, final long preReserveBytes) throws IOException;
}
