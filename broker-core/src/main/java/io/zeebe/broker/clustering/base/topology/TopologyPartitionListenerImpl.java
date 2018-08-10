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
package io.zeebe.broker.clustering.base.topology;

import io.zeebe.protocol.Protocol;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.channel.ActorConditions;
import org.agrona.collections.Int2ObjectHashMap;

public class TopologyPartitionListenerImpl implements TopologyPartitionListener {

  private final Int2ObjectHashMap<SocketAddress> partitionLeaders = new Int2ObjectHashMap<>();
  private volatile SocketAddress systemPartitionLeader;
  private final ActorControl actor;
  private final ActorConditions conditions = new ActorConditions();

  public TopologyPartitionListenerImpl(ActorControl actor) {
    this.actor = actor;
  }

  @Override
  public void onPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member) {
    if (member.getLeaders().contains(partitionInfo)) {
      actor.submit(
          () -> {
            if (partitionInfo.getPartitionId() == Protocol.SYSTEM_PARTITION) {
              updateSystemPartitionLeader(member);
            } else {
              updatePartitionLeader(partitionInfo, member);
            }
            conditions.signalConsumers();
          });
    }
  }

  private void updateSystemPartitionLeader(NodeInfo member) {
    final SocketAddress currentLeader = systemPartitionLeader;

    final SocketAddress managementApiAddress = member.getManagementApiAddress();
    if (currentLeader == null || !currentLeader.equals(managementApiAddress)) {
      systemPartitionLeader = managementApiAddress;
    }
  }

  private void updatePartitionLeader(PartitionInfo partitionInfo, NodeInfo member) {
    actor.submit(
        () -> {
          final SocketAddress currentLeader = partitionLeaders.get(partitionInfo.getPartitionId());

          final SocketAddress subscriptionApiAddress = member.getSubscriptionApiAddress();
          if (currentLeader == null || !currentLeader.equals(subscriptionApiAddress)) {
            final SocketAddress newLeader = subscriptionApiAddress;
            partitionLeaders.put(partitionInfo.getPartitionId(), newLeader);
          }
        });
  }

  public Int2ObjectHashMap<SocketAddress> getPartitionLeaders() {
    return partitionLeaders;
  }

  public SocketAddress getSystemPartitionLeader() {
    return systemPartitionLeader;
  }

  public void addCondition(ActorCondition condition) {
    conditions.registerConsumer(condition);
  }

  public void removeCondition(ActorCondition condition) {
    conditions.removeConsumer(condition);
  }
}
