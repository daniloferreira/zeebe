/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;
import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.util.TopologyClient;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.TomlConfigurationReader;
import io.zeebe.broker.system.configuration.TopicCfg;
import io.zeebe.gateway.ZeebeClient;
import io.zeebe.gateway.api.commands.BrokerInfo;
import io.zeebe.gateway.api.commands.PartitionInfo;
import io.zeebe.gateway.api.commands.Topic;
import io.zeebe.gateway.api.commands.Topics;
import io.zeebe.gateway.impl.ZeebeClientImpl;
import io.zeebe.protocol.Protocol;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.util.Files;
import org.junit.rules.ExternalResource;

public class ClusteringRule extends ExternalResource {

  public static final int DEFAULT_REPLICATION_FACTOR = 1;
  public static final int SYSTEM_TOPIC_REPLICATION_FACTOR = 3;

  public static final String BROKER_1_TOML = "zeebe.cluster.1.cfg.toml";
  public static final String BROKER_2_TOML = "zeebe.cluster.2.cfg.toml";
  public static final String BROKER_3_TOML = "zeebe.cluster.3.cfg.toml";
  public static final String BROKER_4_TOML = "zeebe.cluster.4.cfg.toml";

  // internal
  private ZeebeClient zeebeClient;
  private TopologyClient topologyClient;

  private String[] brokerConfigFiles;
  protected final List<Broker> brokers = new ArrayList<>();
  private List<AutoCloseable> closeables = new ArrayList<>();

  public ClusteringRule() {
    this(new String[] {BROKER_1_TOML, BROKER_2_TOML, BROKER_3_TOML});
  }

  public ClusteringRule(final String[] brokerConfigFiles) {
    this.brokerConfigFiles = brokerConfigFiles;
  }

  @Override
  protected void before() {
    for (String brokerConfigFile : brokerConfigFiles) {
      startBroker(brokerConfigFile);
    }

    zeebeClient =
        ZeebeClient.newClientBuilder()
            .brokerContactPoint(
                brokers.get(0).getConfig().getNetwork().getClient().toSocketAddress().toString())
            .build();

    topologyClient = new TopologyClient((ZeebeClientImpl) zeebeClient);

    waitForInternalSystemAndReplicationFactor();

    final Broker leaderBroker = brokers.get(0);
    final BrokerCfg brokerConfiguration = leaderBroker.getBrokerContext().getBrokerConfiguration();
    final TopicCfg defaultTopicCfg = brokerConfiguration.getTopics().get(0);
    final int partitions = defaultTopicCfg.getPartitions();
    final int replicationFactor = defaultTopicCfg.getReplicationFactor();

    waitForTopicPartitionReplicationFactor(DEFAULT_TOPIC, partitions, replicationFactor);

    waitUntilBrokersInTopology(brokers);
  }

  @Override
  protected void after() {
    final int size = closeables.size();
    for (int i = size - 1; i >= 0; i--) {
      try {
        closeables.remove(i).close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void waitUntilBrokersInTopology(final List<Broker> brokers) {
    final Set<SocketAddress> addresses =
        brokers
            .stream()
            .map(b -> b.getConfig().getNetwork().getClient().toSocketAddress())
            .collect(Collectors.toSet());

    waitForTopology(
        topology ->
            topology
                .stream()
                .map(b -> new SocketAddress(b.getHost(), b.getPort()))
                .collect(Collectors.toSet())
                .containsAll(addresses));
  }

  private void waitForInternalSystemAndReplicationFactor() {
    waitForTopicPartitionReplicationFactor("internal-system", 1, SYSTEM_TOPIC_REPLICATION_FACTOR);
  }

  /**
   * Returns the current leader for the given partition.
   *
   * @param partition
   * @return
   */
  public BrokerInfo getLeaderForPartition(final int partition) {
    return doRepeatedly(
            () -> {
              final List<BrokerInfo> brokers =
                  zeebeClient.newTopologyRequest().send().join().getBrokers();
              return extractPartitionLeader(brokers, partition);
            })
        .until(Optional::isPresent)
        .get();
  }

  public SocketAddress getLeaderAddressForPartition(final int partition) {
    final BrokerInfo info = getLeaderForPartition(partition);
    return new SocketAddress(info.getHost(), info.getPort());
  }

  public BrokerInfo getFollowerForPartition(final int partitionId) {
    return doRepeatedly(
            () -> {
              final List<BrokerInfo> brokers =
                  zeebeClient.newTopologyRequest().send().join().getBrokers();
              return extractPartitionFollower(brokers, partitionId);
            })
        .until(Optional::isPresent)
        .orElse(null);
  }

  public SocketAddress getFollowerAddressForPartition(final int partition) {
    final BrokerInfo info = getFollowerForPartition(partition);
    return new SocketAddress(info.getHost(), info.getPort());
  }

  /** @return a node which is not leader of any partition, or null if none exist */
  public Broker getFollowerOnly() {
    for (final Broker broker : brokers) {
      if (getBrokersLeadingPartitions(broker.getConfig().getNetwork().getClient().toSocketAddress())
          .isEmpty()) {
        return broker;
      }
    }

    return null;
  }

  private Optional<BrokerInfo> extractPartitionLeader(
      final List<BrokerInfo> brokers, final int partition) {
    return brokers
        .stream()
        .filter(
            b ->
                b.getPartitions()
                    .stream()
                    .anyMatch(p -> p.getPartitionId() == partition && p.isLeader()))
        .findFirst();
  }

  private Optional<BrokerInfo> extractPartitionFollower(
      final List<BrokerInfo> brokers, final int partition) {
    return brokers
        .stream()
        .filter(
            b ->
                b.getPartitions()
                    .stream()
                    .anyMatch(p -> p.getPartitionId() == partition && !p.isLeader()))
        .findFirst();
  }

  /**
   * Wait for a topic with the given partition count in the cluster.
   *
   * <p>This method returns to the user, if the topic and the partitions are created and the
   * replication factor was reached for each partition. Besides that the topic request needs to be
   * return the created topic.
   *
   * <p>The replication factor is per default the number of current brokers in the cluster, see
   * {@link #DEFAULT_REPLICATION_FACTOR}.
   *
   * @param partitionCount to number of partitions for the new topic
   * @return the created topic
   */
  public Topic waitForTopic(int partitionCount) {
    return waitForTopic(partitionCount, DEFAULT_REPLICATION_FACTOR);
  }

  public Topic waitForTopic(int partitionCount, int replicationFactor) {
    waitForTopicPartitionReplicationFactor(DEFAULT_TOPIC, partitionCount, replicationFactor);

    return waitForTopicAvailability(DEFAULT_TOPIC);
  }

  private boolean hasPartitionsWithReplicationFactor(
      final List<BrokerInfo> brokers,
      final String topicName,
      final int partitionCount,
      final int replicationFactor) {
    final AtomicLong leaders = new AtomicLong();
    final AtomicLong followers = new AtomicLong();

    brokers
        .stream()
        .flatMap(b -> b.getPartitions().stream())
        .filter(p -> p.getTopicName().equals(topicName))
        .forEach(
            p -> {
              if (p.isLeader()) {
                leaders.getAndIncrement();
              } else {
                followers.getAndIncrement();
              }
            });

    return leaders.get() >= partitionCount
        && followers.get() >= partitionCount * (replicationFactor - 1);
  }

  public void waitForTopicPartitionReplicationFactor(
      final String topicName, final int partitionCount, final int replicationFactor) {
    waitForTopology(
        topology ->
            hasPartitionsWithReplicationFactor(
                topology, topicName, partitionCount, replicationFactor));
  }

  public Topic getInternalSystemTopic() {
    return waitForTopicAvailability(Protocol.SYSTEM_TOPIC);
  }

  private Topic waitForTopicAvailability(final String topicName) {
    return doRepeatedly(
            () -> {
              final Topics topics = zeebeClient.newTopicsRequest().send().join();
              return topics
                  .getTopics()
                  .stream()
                  .filter(topic -> topicName.equals(topic.getName()))
                  .findAny();
            })
        .until(Optional::isPresent)
        .get();
  }

  private void startBroker(final String configFile) {
    final File base = Files.newTemporaryFolder();

    final InputStream config = this.getClass().getClassLoader().getResourceAsStream(configFile);
    final BrokerCfg brokerCfg = TomlConfigurationReader.read(config);
    EmbeddedBrokerRule.assignSocketAddresses(brokerCfg);

    if (!brokers.isEmpty()) {
      brokerCfg
          .getCluster()
          .setInitialContactPoints(
              new String[] {
                brokers.get(0).getConfig().getNetwork().getManagement().toSocketAddress().toString()
              });
    }

    final Broker broker = new Broker(brokerCfg, base.getAbsolutePath(), null);

    brokers.add(broker);

    closeables.add(() -> FileUtil.deleteFolder(base.getAbsolutePath()));
    closeables.add(broker);
  }

  /**
   * Restarts broker, if the broker is still running it will be closed before.
   *
   * <p>Returns to the user if the broker is back in the cluster.
   */
  public void restartBroker(final int brokerId) {
    final Broker broker = brokers.get(brokerId);
    if (broker != null) {
      stopBroker(broker);
    }

    startBroker(brokerConfigFiles[brokerId]);

    waitUntilBrokerIsAddedToTopology(broker.getConfig().getNetwork().getClient().toSocketAddress());
    waitForInternalSystemAndReplicationFactor();
  }

  public void restartBroker(Broker broker) {
    restartBroker(brokers.indexOf(broker));
  }

  private void waitUntilBrokerIsAddedToTopology(final SocketAddress socketAddress) {
    waitForTopology(
        topology ->
            topology
                .stream()
                .anyMatch(
                    b ->
                        b.getHost().equals(socketAddress.host())
                            && b.getPort() == socketAddress.port()));
  }

  /**
   * Returns for a given broker the leading partition id's.
   *
   * @param socketAddress
   * @return
   */
  public List<Integer> getBrokersLeadingPartitions(final SocketAddress socketAddress) {
    return zeebeClient
        .newTopologyRequest()
        .send()
        .join()
        .getBrokers()
        .stream()
        .filter(
            b -> b.getHost().equals(socketAddress.host()) && b.getPort() == socketAddress.port())
        .flatMap(broker -> broker.getPartitions().stream())
        .filter(PartitionInfo::isLeader)
        .map(PartitionInfo::getPartitionId)
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of available brokers in a cluster.
   *
   * @return
   */
  public List<SocketAddress> getBrokersInCluster() {
    return zeebeClient
        .newTopologyRequest()
        .send()
        .join()
        .getBrokers()
        .stream()
        .map(b -> new SocketAddress(b.getHost(), b.getPort()))
        .collect(Collectors.toList());
  }

  public List<Broker> getBrokers() {
    return brokers;
  }

  public Broker getBroker(final SocketAddress address) {
    for (Broker broker : brokers) {
      if (address.equals(broker.getConfig().getNetwork().getClient().toSocketAddress())) {
        return broker;
      }
    }

    return null;
  }

  public SocketAddress[] getOtherBrokers(final String address) {
    return getOtherBrokers(SocketAddress.from(address));
  }

  public SocketAddress[] getOtherBrokers(final SocketAddress address) {
    return brokers
        .stream()
        .map(b -> b.getConfig().getNetwork().getClient().toSocketAddress())
        .filter(a -> !address.equals(a))
        .toArray(SocketAddress[]::new);
  }

  public SocketAddress[] getOtherBrokers(final int brokerId) {
    return getOtherBrokers(
        brokers.get(brokerId).getConfig().getNetwork().getClient().toSocketAddress());
  }

  /**
   * Returns the count of partition leaders for a given topic.
   *
   * @param topic
   * @return
   */
  public long getPartitionLeaderCountForTopic(final String topic) {

    return zeebeClient
        .newTopologyRequest()
        .send()
        .join()
        .getBrokers()
        .stream()
        .flatMap(broker -> broker.getPartitions().stream())
        .filter(p -> p.getTopicName().equals(topic) && p.isLeader())
        .count();
  }

  /**
   * Stops broker with the given socket address.
   *
   * <p>Returns to the user if the broker was stopped and new leader for the partitions are chosen.
   */
  public void stopBroker(final String address) {
    final SocketAddress socketAddress = SocketAddress.from(address);

    for (Broker broker : brokers) {
      if (broker.getConfig().getNetwork().getClient().toSocketAddress().equals(socketAddress)) {
        stopBroker(broker);
        break;
      }
    }
  }

  public void stopBroker(final int brokerId) {
    stopBroker(brokers.get(brokerId));
  }

  public void stopBroker(final Broker broker) {
    final SocketAddress socketAddress =
        broker.getConfig().getNetwork().getClient().toSocketAddress();
    final List<Integer> brokersLeadingPartitions = getBrokersLeadingPartitions(socketAddress);

    final boolean removed = brokers.remove(broker);
    assertThat(removed)
        .withFailMessage("Unable to find broker to remove %s", socketAddress)
        .isTrue();

    broker.close();

    waitForNewLeaderOfPartitions(brokersLeadingPartitions, socketAddress);
    waitUntilBrokerIsRemovedFromTopology(socketAddress);
  }

  private void waitUntilBrokerIsRemovedFromTopology(final SocketAddress socketAddress) {
    waitForTopology(
        topology ->
            topology
                .stream()
                .noneMatch(
                    b ->
                        b.getHost().equals(socketAddress.host())
                            && b.getPort() == socketAddress.port()));
  }

  private void waitForNewLeaderOfPartitions(
      final List<Integer> partitions, final SocketAddress oldLeader) {
    waitForTopology(
        topology ->
            topology
                .stream()
                .filter(
                    b -> !(b.getHost().equals(oldLeader.host()) && b.getPort() == oldLeader.port()))
                .flatMap(broker -> broker.getPartitions().stream())
                .filter(PartitionInfo::isLeader)
                .map(PartitionInfo::getPartitionId)
                .collect(Collectors.toSet())
                .containsAll(partitions));
  }

  public void waitForTopology(final Function<List<BrokerInfo>, Boolean> topologyPredicate) {
    waitUntil(
        () ->
            brokers
                .stream()
                .allMatch(
                    broker -> {
                      final List<BrokerInfo> topology =
                          topologyClient.requestTopologyFromBroker(
                              broker.getConfig().getNetwork().getClient().toSocketAddress());
                      // printTopology(topology);
                      return topologyPredicate.apply(topology);
                    }),
        250);
  }

  public SocketAddress getClientAddress() {
    return brokers.get(0).getConfig().getNetwork().getClient().toSocketAddress();
  }
}
