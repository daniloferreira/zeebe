/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.system.workflow.repository.api.management;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.system.workflow.repository.data.DeploymentRecord;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerRequestHandler;
import io.zeebe.transport.ServerResponse;
import io.zeebe.util.sched.ActorControl;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;

public class PushDeploymentRequestHandler implements ServerRequestHandler {

  private static final Logger LOG = Loggers.WORKFLOW_REPOSITORY_LOGGER;

  private final LogStreamRecordWriter logStreamWriter = new LogStreamWriterImpl();
  private final RecordMetadata recordMetadata = new RecordMetadata();

  private final Int2ObjectHashMap<Partition> leaderPartitions;
  private final ActorControl actor;

  public PushDeploymentRequestHandler(
      Int2ObjectHashMap<Partition> leaderPartitions, ActorControl actor) {
    this.leaderPartitions = leaderPartitions;
    this.actor = actor;
  }

  @Override
  public boolean onRequest(
      ServerOutput output,
      RemoteAddress remoteAddress,
      DirectBuffer buffer,
      int offset,
      int length,
      long requestId) {

    final PushDeploymentRequest pushDeploymentRequest = new PushDeploymentRequest();
    pushDeploymentRequest.wrap(buffer, offset, length);

    final PushDeploymentResponse pushResponse = new PushDeploymentResponse();
    final long deploymentKey = pushDeploymentRequest.deploymentKey();

    LOG.debug("Got deployment push request for deployment {}.", deploymentKey);

    pushResponse.deploymentKey(deploymentKey);
    final int partitionId = pushDeploymentRequest.partitionId();
    pushResponse.partitionId(partitionId);

    LOG.debug("Send push deployment response back.");

    final Partition partition = leaderPartitions.get(partitionId);
    if (partition != null) {
      LOG.debug("Leader for partition {}, write deployment event.", partitionId);

      final DeploymentRecord deploymentRecord = new DeploymentRecord();
      final DirectBuffer directBuffer = pushDeploymentRequest.deployment();
      deploymentRecord.wrap(directBuffer);

      actor.runUntilDone(
          () -> {
            final boolean success =
                writeEvent(
                    partitionId,
                    deploymentKey,
                    ValueType.DEPLOYMENT,
                    DeploymentIntent.CREATED,
                    deploymentRecord);
            if (success) {
              LOG.debug("Deployment CREATED Event was written on partition {}", partitionId);
              actor.done();

              final ServerResponse serverResponse =
                  new ServerResponse()
                      .writer(pushResponse)
                      .requestId(requestId)
                      .remoteAddress(remoteAddress);

              actor.runUntilDone(
                  () -> {
                    if (output.sendResponse(serverResponse)) {
                      actor.done();
                      LOG.debug("Send response back to partition 1.");
                    } else {
                      actor.yield();
                    }
                  });
            } else {
              actor.yield();
            }
          });

    } else {
      LOG.debug("Not leader for partition {}", partitionId);
      return false;
    }

    return true;
  }

  private boolean writeEvent(
      int partitionId, long key, ValueType valueType, Intent intent, UnpackedObject event) {

    final Partition partition = leaderPartitions.get(partitionId);
    if (partition == null) {
      // ignore message if you are not the leader of the partition
      return true;
    }

    logStreamWriter.wrap(partition.getLogStream());

    recordMetadata.reset().recordType(RecordType.EVENT).valueType(valueType).intent(intent);

    final long position =
        logStreamWriter.key(key).metadataWriter(recordMetadata).valueWriter(event).tryWrite();

    return position > 0;
  }
}
