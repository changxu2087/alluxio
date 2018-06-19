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

package alluxio.worker.block.meta;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the metadata of a block in Alluxio managed storage.
 */
@ThreadSafe
public final class BlockMeta extends AbstractBlockMeta {
  private final long mBlockSize;
  private final boolean mMustReserve;
  private final long mPreReserveBytes;

  /**
   * Creates a new instance of {@link BlockMeta}.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param dir the parent directory
   */
  public BlockMeta(long blockId, long blockSize, StorageDir dir) {
    super(blockId, dir);
    mBlockSize = blockSize;
    mMustReserve = false;
    mPreReserveBytes = 0;
  }

  /**
   * Creates a new instance of {@link BlockMeta}.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param dir the parent directory
   * @param isMustReserve whether must reserve
   * @param preReserveBytes the size of pre reserved block in byte
   */
  public BlockMeta(long blockId, long blockSize, StorageDir dir, boolean isMustReserve, long preReserveBytes) {
    super(blockId, dir);
    mBlockSize = blockSize;
    mMustReserve = isMustReserve;
    mPreReserveBytes = preReserveBytes;
  }

  /**
   * Creates a new instance of {@link BlockMeta} from {@link TempBlockMeta}.
   *
   * @param tempBlock uncommitted block metadata
   */
  public BlockMeta(TempBlockMeta tempBlock) {
    super(tempBlock.getBlockId(), tempBlock.getParentDir());
    // NOTE: TempBlockMeta must be committed after the actual data block file is moved.
    mBlockSize = new File(tempBlock.getCommitPath()).length();
    mMustReserve = tempBlock.isMustReserve();
    mPreReserveBytes = tempBlock.getPreReserveBytes();
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  @Override
  public String getPath() {
    return commitPath(mDir, mBlockId);
  }

  /**
   * @return true is must reserve, false otherwise
   */
  public boolean isMustReserve() {
    return mMustReserve;
  }

  /**
   * @return the size of pre Reserve Bytes
   */
  public long getPreReserveBytes() {
    return mPreReserveBytes;
  }
}
