/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */
package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shawn-pc on 4/10/18.
 * Delete workers from the cluster by the hosts in args and transfer partial blocks from the deleted workers to
 * the rest workers or under file system.
 */
@ThreadSafe
public class DeleteWorkerCommand extends AbstractFileSystemCommand{
    public DeleteWorkerCommand(FileSystem fs) {
        super(fs);
    }

    @Override
    public String getCommandName() {
        return "deleteworker";
    }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    List<String> hosts = Arrays.asList(args);
    Map<String, List<Long>> nonPersisted = new HashMap<>();
    for (String host : hosts) {
      nonPersisted.put(host, new ArrayList<>());
    }
    decommission(hosts, new AlluxioURI(AlluxioURI.SEPARATOR), nonPersisted);
    try (CloseableResource<BlockMasterClient> client =
        FileSystemContext.INSTANCE.acquireBlockMasterClientResource()) {
      client.get().deleteWorker(nonPersisted);
    }
    return 0;
  }

    @Override
    public String getUsage() {
        return "deleteworker <host1> [host2] ... [hostn]";
    }

    @Override
    public String getDescription() {
        return "Delete workers from the cluster by the hosts in args and " +
                "transfer partial blocks from the deleted workers to the rest workers or under file system.";
    }

    @Override
    protected int getNumOfArgs() {
        return 1;
    }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length < 1) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM_INSUFFICIENT
          .getMessage(getCommandName(), 1, args.length));
    }
  }

  private void decommission(List<String> hosts, AlluxioURI filePath,
      Map<String, List<Long>> nonPersistFile) throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      List<String> errorMessages = new ArrayList<>();
      for (URIStatus uriStatus : statuses) {
        try {
          decommission(hosts, new AlluxioURI(uriStatus.getPath()), nonPersistFile);
        } catch (Exception e) {
          errorMessages.add(e.getMessage());
        }
      }
      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else if (!status.isPersisted()) {
      List<FileBlockInfo> blockInfos = status.getFileBlockInfos();
      for (FileBlockInfo blockInfo : blockInfos) {
        List<String> locationHost = new ArrayList<>();
        for (BlockLocation blockLocation : blockInfo.getBlockInfo().getLocations()) {
          locationHost.add(blockLocation.getWorkerAddress().getHost());
          if (hosts.containsAll(locationHost)) {
            nonPersistFile.get(locationHost.get(0)).add(status.getFileId());
            return;
          }
        }
      }
    }
  }
}

