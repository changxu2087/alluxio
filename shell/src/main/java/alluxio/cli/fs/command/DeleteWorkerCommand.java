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
package alluxio.cli.fs.command;

import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shawn-pc on 4/10/18.
 * Delete workers from the cluster by the hosts in args and transfer partial blocks from the deleted workers to
 * the rest workers.
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
        List<String> hosts = new ArrayList<>();
        for (String host : args) {
            hosts.add(host);
        }
        try(CloseableResource<BlockMasterClient> client = FileSystemContext.INSTANCE.acquireBlockMasterClientResource()) {
            client.get().deleteWorker(hosts);
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
                "transfer partial blocks from the deleted workers to the rest workers.";
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
}

