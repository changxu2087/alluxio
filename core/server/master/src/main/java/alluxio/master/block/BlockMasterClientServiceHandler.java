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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.block.options.RemoveWorkerOptions;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.DeleteWorkerTOptions;
import alluxio.thrift.DeleteWorkerTResponse;
import alluxio.thrift.GetBlockInfoTOptions;
import alluxio.thrift.GetBlockInfoTResponse;
import alluxio.thrift.GetCapacityBytesTOptions;
import alluxio.thrift.GetCapacityBytesTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.GetUsedBytesTOptions;
import alluxio.thrift.GetUsedBytesTResponse;
import alluxio.thrift.GetWorkerInfoListTOptions;
import alluxio.thrift.GetWorkerInfoListTResponse;
import alluxio.thrift.RemoveWorkerTOptions;
import alluxio.thrift.RemoveWorkerTResponse;
import alluxio.thrift.ValidateAndReserveTOptions;
import alluxio.thrift.ValidateAndReserveTResponse;
import alluxio.thrift.WorkerInfo;
import alluxio.thrift.WorkerNetAddress;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class BlockMasterClientServiceHandler implements BlockMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterClientServiceHandler.class);

  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterClientServiceHandler}.
   *
   * @param blockMaster the {@link BlockMaster} the handler uses internally
   */
  BlockMasterClientServiceHandler(BlockMaster blockMaster) {
    Preconditions.checkNotNull(blockMaster, "blockMaster");
    mBlockMaster = blockMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public GetWorkerInfoListTResponse getWorkerInfoList(GetWorkerInfoListTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<GetWorkerInfoListTResponse>() {
      @Override
      public GetWorkerInfoListTResponse call() throws AlluxioException, AlluxioStatusException {
        List<WorkerInfo> workerInfos = new ArrayList<>();
        for (alluxio.wire.WorkerInfo workerInfo : mBlockMaster.getWorkerInfoList()) {
          workerInfos.add(ThriftUtils.toThrift(workerInfo));
        }
        return new GetWorkerInfoListTResponse(workerInfos);
      }

      @Override
      public String toString() {
        return String.format("getWorkerInfoList: options=%s", options);
      }
    });
  }

  @Override
  public RemoveWorkerTResponse removeWorker(final WorkerNetAddress address,
                                            final RemoveWorkerTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<RemoveWorkerTResponse>() {
      @Override
      public RemoveWorkerTResponse call() throws AlluxioException {
        mBlockMaster.removeWorker(ThriftUtils.fromThrift(address), new RemoveWorkerOptions(options));
        return new RemoveWorkerTResponse();
      }

      @Override
      public String toString() {
        return String.format("removeWorker: address=%s, options=%s", address, options);
      }
    });
  }

  @Override
  public DeleteWorkerTResponse deleteWorker(final List<String> hosts, DeleteWorkerTOptions options) throws AlluxioTException {
    LOG.debug("server delete worker");
    System.out.println("server delete worker");
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<DeleteWorkerTResponse>() {
      @Override
      public DeleteWorkerTResponse call() throws AlluxioException, IOException {
        mBlockMaster.deleteWorker(hosts);
        return new DeleteWorkerTResponse();
      }

      @Override
      public String toString() {
        return String.format("deleteWorker: hosts=%s, options=%s", hosts, options);
      }
    });
  }

  @Override
  public ValidateAndReserveTResponse validateAndReserve(WorkerNetAddress address, long preReserveBytes,
      ValidateAndReserveTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<ValidateAndReserveTResponse>() {
      @Override
      public ValidateAndReserveTResponse call() throws AlluxioException, UnavailableException {
        List<WorkerInfo> workerInfos = new ArrayList<>();
        for (alluxio.wire.WorkerInfo workerInfo : mBlockMaster
            .validateAndReserve(ThriftUtils.fromThrift(address), preReserveBytes)) {
          workerInfos.add(ThriftUtils.toThrift(workerInfo));
        }
        return new ValidateAndReserveTResponse(workerInfos);
      }

      public String toString() {
        return String.format("validateAndReserve: address=%s, preReserveBytes=%s, options=%s", address,
                preReserveBytes, options);
      }
    });
  }

  @Override
  public GetCapacityBytesTResponse getCapacityBytes(GetCapacityBytesTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<GetCapacityBytesTResponse>() {
      @Override
      public GetCapacityBytesTResponse call() throws AlluxioException {
        return new GetCapacityBytesTResponse(mBlockMaster.getCapacityBytes());
      }

      @Override
      public String toString() {
        return String.format("getCapacityBytes: options=%s", options);
      }
    });
  }

  @Override
  public GetUsedBytesTResponse getUsedBytes(GetUsedBytesTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<GetUsedBytesTResponse>() {
      @Override
      public GetUsedBytesTResponse call() throws AlluxioException {
        return new GetUsedBytesTResponse(mBlockMaster.getUsedBytes());
      }

      @Override
      public String toString() {
        return String.format("getUsedBytes: options=%s", options);
      }
    });
  }

  @Override
  public GetBlockInfoTResponse getBlockInfo(final long blockId, GetBlockInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<GetBlockInfoTResponse>() {
      @Override
      public GetBlockInfoTResponse call() throws AlluxioException, AlluxioStatusException {
        return new GetBlockInfoTResponse(ThriftUtils.toThrift(mBlockMaster.getBlockInfo(blockId)));
      }

      @Override
      public String toString() {
        return String.format("getBlockInfo: blockId=%s, options=%s", blockId, options);
      }
    });
  }
}
