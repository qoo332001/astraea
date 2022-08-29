/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.web;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartitionReplica;

public class ThrottleHandler implements Handler {
  private final Admin admin;

  public ThrottleHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Response get(Channel channel) {
    return get();
  }

  private Response get() {
    final var brokers =
        admin.brokers().entrySet().stream()
            .map(
                entry -> {
                  final var egress =
                      entry
                          .getValue()
                          .value("leader.replication.throttled.rate")
                          .map(Long::valueOf)
                          .orElse(null);
                  final var ingress =
                      entry
                          .getValue()
                          .value("follower.replication.throttled.rate")
                          .map(Long::valueOf)
                          .orElse(null);
                  return new BrokerThrottle(entry.getKey(), ingress, egress);
                })
            .collect(Collectors.toUnmodifiableSet());
    final var topicConfigs = admin.topics();
    final var leaderTargets =
        topicConfigs.entrySet().stream()
            .map(
                entry ->
                    toReplicaSet(
                        entry.getKey(),
                        entry.getValue().value("leader.replication.throttled.replicas").orElse("")))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());
    final var followerTargets =
        topicConfigs.entrySet().stream()
            .map(
                entry ->
                    toReplicaSet(
                        entry.getKey(),
                        entry
                            .getValue()
                            .value("follower.replication.throttled.replicas")
                            .orElse("")))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());

    return new ThrottleSetting(brokers, simplify(leaderTargets, followerTargets));
  }

  /**
   * break apart the {@code throttled.replica} string setting into a set of topic/partition/replicas
   */
  private Set<TopicPartitionReplica> toReplicaSet(String topic, String throttledReplicas) {
    if (throttledReplicas.isEmpty()) return Set.of();

    // TODO: support for wildcard throttle might be implemented in the future, see
    // https://github.com/skiptests/astraea/issues/625
    if (throttledReplicas.equals("*"))
      throw new UnsupportedOperationException("This API doesn't support wildcard throttle");

    return Arrays.stream(throttledReplicas.split(","))
        .map(pair -> pair.split(":"))
        .map(
            pair ->
                TopicPartitionReplica.of(
                    topic, Integer.parseInt(pair[0]), Integer.parseInt(pair[1])))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Given a series of leader/follower throttle config, this method attempts to reduce its size into
   * the simplest form by merging any targets with a common topic/partition/replica scope throttle
   * target.
   */
  private Set<TopicThrottle> simplify(
      Set<TopicPartitionReplica> leaders, Set<TopicPartitionReplica> followers) {
    var commonReplicas =
        leaders.stream().filter(followers::contains).collect(Collectors.toUnmodifiableSet());

    var simplifiedReplicas =
        commonReplicas.stream()
            .map(
                replica ->
                    new TopicThrottle(
                        replica.topic(), replica.partition(), replica.brokerId(), null));
    var leaderReplicas =
        leaders.stream()
            .filter(replica -> !commonReplicas.contains(replica))
            .map(
                replica ->
                    new TopicThrottle(
                        replica.topic(),
                        replica.partition(),
                        replica.brokerId(),
                        LogIdentity.leader));
    var followerReplicas =
        followers.stream()
            .filter(replica -> !commonReplicas.contains(replica))
            .map(
                replica ->
                    new TopicThrottle(
                        replica.topic(),
                        replica.partition(),
                        replica.brokerId(),
                        LogIdentity.follower));

    return Stream.concat(Stream.concat(simplifiedReplicas, leaderReplicas), followerReplicas)
        .collect(Collectors.toUnmodifiableSet());
  }

  static class ThrottleSetting implements Response {

    final Collection<BrokerThrottle> brokers;
    final Collection<TopicThrottle> topics;

    ThrottleSetting(Collection<BrokerThrottle> brokers, Collection<TopicThrottle> topics) {
      this.brokers = brokers;
      this.topics = topics;
    }
  }

  static class BrokerThrottle {
    final int id;
    final Long ingress;
    final Long egress;

    BrokerThrottle(int id, Long ingress, Long egress) {
      this.id = id;
      this.ingress = ingress;
      this.egress = egress;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BrokerThrottle that = (BrokerThrottle) o;
      return id == that.id
          && Objects.equals(ingress, that.ingress)
          && Objects.equals(egress, that.egress);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, ingress, egress);
    }

    @Override
    public String toString() {
      return "BrokerThrottle{"
          + "broker="
          + id
          + ", ingress="
          + ingress
          + ", egress="
          + egress
          + '}';
    }
  }

  static class TopicThrottle {
    final String name;
    final Integer partition;
    final Integer broker;
    final String type;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TopicThrottle that = (TopicThrottle) o;
      return Objects.equals(name, that.name)
          && Objects.equals(partition, that.partition)
          && Objects.equals(broker, that.broker)
          && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, partition, broker, type);
    }

    TopicThrottle(String name, Integer partition, Integer broker, LogIdentity identity) {
      this.name = name;
      this.partition = partition;
      this.broker = broker;
      this.type = (identity == null) ? null : identity.name();
    }

    @Override
    public String toString() {
      return "ThrottleTarget{"
          + "name='"
          + name
          + '\''
          + ", partition="
          + partition
          + ", broker="
          + broker
          + ", type="
          + type
          + '}';
    }
  }

  enum LogIdentity {
    leader,
    follower
  }
}