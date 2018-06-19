namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct WorkerInfo {
  1: i64 id
  2: common.WorkerNetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 startTimeMs
  8: i64 unavailableBytes
}

struct GetBlockInfoTOptions {}
struct GetBlockInfoTResponse {
  1: common.BlockInfo blockInfo
}

struct GetCapacityBytesTOptions {}
struct GetCapacityBytesTResponse {
  1: i64 bytes
}

struct GetUsedBytesTOptions {}
struct GetUsedBytesTResponse {
  1: i64 bytes
}

struct GetWorkerInfoListTOptions {}
struct GetWorkerInfoListTResponse {
  1: list<WorkerInfo> workerInfoList
}

struct RemoveWorkerTOptions {
  1: optional bool transferCache
  2: optional bool persist
}
struct RemoveWorkerTResponse {}

struct DeleteWorkerTOptions {}
struct DeleteWorkerTResponse {}

struct ValidateAndReserveTOptions {}
struct ValidateAndReserveTResponse {
  1: list<WorkerInfo> workerInfoList
}

/**
 * This interface contains block master service endpoints for Alluxio clients.
 */
service BlockMasterClientService extends common.AlluxioService {

  /**
   * Returns the block information for the given block id.
   */
  GetBlockInfoTResponse getBlockInfo(
    /** the id of the block */  1: i64 blockId,
    /** the method options */ 2: GetBlockInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the capacity (in bytes).
   */
  GetCapacityBytesTResponse getCapacityBytes(
    /** the method options */ 1: GetCapacityBytesTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns the used storage (in bytes).
   */
  GetUsedBytesTResponse getUsedBytes(
    /** the method options */ 1: GetUsedBytesTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns a list of workers information.
   */
  GetWorkerInfoListTResponse getWorkerInfoList(
    /** the method options */ 1: GetWorkerInfoListTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Remove Worker from the cluster.
   *
   * @param address The address of the worker to remove
   * @param options The remove worker options
   */
  RemoveWorkerTResponse removeWorker(
    /** the address of the removed worker */  1: common.WorkerNetAddress address,
    /** the method options */ 2: RemoveWorkerTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Delete Workers from the cluster.
   */
  DeleteWorkerTResponse deleteWorker(
    /** the address of the delete worker */ 1: list<string> hosts,
    /** the method options */ 2: DeleteWorkerTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Validate whether the worker of this address can be used as block store, and if it could,
   * reserve block size space on the worker. If not, return the latest information of all workers
   */
  ValidateAndReserveTResponse validateAndReserve(
    /** the address of the validated worker */ 1: common.WorkerNetAddress address,
    /** the size that need to be pre reserved */ 2: i64 preReserveBytes,
    /** the method options */ 3: ValidateAndReserveTOptions options,
  ) throws (1: exception.AlluxioTException e)
}

struct BlockHeartbeatTOptions {}
struct BlockHeartbeatTResponse {
  1: common.Command command
}

struct CommitBlockTOptions {}
struct CommitBlockTResponse {}

struct GetWorkerIdTOptions {}
struct GetWorkerIdTResponse {
  1: i64 workerId
}

struct RegisterWorkerTOptions {}
struct RegisterWorkerTResponse {}

/**
 * This interface contains block master service endpoints for Alluxio workers.
 */
service BlockMasterWorkerService extends common.AlluxioService {

  /**
   * Periodic block worker heartbeat returns an optional command for the block worker to execute.
   */
  BlockHeartbeatTResponse blockHeartbeat(
    /** the id of the worker */ 1: i64 workerId,
    /** the map of space used in bytes on all tiers */ 2: map<string, i64> usedBytesOnTiers,
    /** the list of removed block ids */ 3: list<i64> removedBlockIds,
    /** the map of added blocks on all tiers */ 4: map<string, list<i64>> addedBlocksOnTiers,
    /** the method options */ 5: BlockHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Marks the given block as committed.
   */
  CommitBlockTResponse commitBlock(
    /** the id of the worker */  1: i64 workerId,
    /** the space used in bytes on the target tier */ 2: i64 usedBytesOnTier,
    /** the alias of the target tier */ 3: string tierAlias,
    /** the id of the block being committed */ 4: i64 blockId,
    /** the length of the block being committed */ 5: i64 length,
    /** whether or not the block must be reserved */ 6: bool isMustReserve
    /** the size of pre reserved block in byte */ 7:  i64 preReserveBytes
    /** the method options */ 8: CommitBlockTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns a worker id for the given network address.
   */
  GetWorkerIdTResponse getWorkerId(
    /** the worker network address */ 1: common.WorkerNetAddress workerNetAddress,
    /** the method options */ 2: GetWorkerIdTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Registers a worker.
   */
  RegisterWorkerTResponse registerWorker(
    /** the id of the worker */  1: i64 workerId,
    /** the list of storage tiers */  2: list<string> storageTiers,
    /** the map of total bytes on each tier */  3: map<string, i64> totalBytesOnTiers,
    /** the map of used bytes on each tier */  4: map<string, i64> usedBytesOnTiers,
    /** the map of list of blocks on each tier */  5: map<string, list<i64>> currentBlocksOnTiers,
    /** the unavailable space of the worker in byte */  6: i64 unavailableBytes,
    /** the method options */ 7: RegisterWorkerTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
