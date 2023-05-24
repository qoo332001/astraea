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
package org.astraea.common.cost;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasMaxRate;

public class MigrationCost {

  public final String name;
  public final Map<Integer, Long> brokerCosts;
  public static final String TO_SYNC_BYTES = "record size to sync (bytes)";
  public static final String TO_FETCH_BYTES = "record size to fetch (bytes)";
  public static final String REPLICA_LEADERS_TO_ADDED = "leader number to add";
  public static final String REPLICA_LEADERS_TO_REMOVE = "leader number to remove";
  public static final String CHANGED_REPLICAS = "changed replicas";
  public static final String PARTITION_MIGRATED_TIME = "partition migrated time";

  public static List<MigrationCost> migrationCosts(
      ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var migrateInBytes = recordSizeToSync(before, after);
    var migrateOutBytes = recordSizeToFetch(before, after);

    System.out.println("migration in sum: " + migrateInBytes.values().stream().mapToLong(x -> x).sum());
    System.out.println("migration out sum: " + migrateOutBytes.values().stream().mapToLong(x -> x).sum());

    var migrateReplicaNum = replicaNumChanged(before, after);
    var migrateInLeader = replicaLeaderToAdd(before, after);
    var migrateOutLeader = replicaLeaderToRemove(before, after);
    var brokerMigrationSecond = brokerMigrationSecond(before, after, clusterBean);
    return List.of(
        new MigrationCost(TO_SYNC_BYTES, migrateInBytes),
        new MigrationCost(TO_FETCH_BYTES, migrateOutBytes),
        new MigrationCost(CHANGED_REPLICAS, migrateReplicaNum),
        new MigrationCost(PARTITION_MIGRATED_TIME, brokerMigrationSecond),
        new MigrationCost(REPLICA_LEADERS_TO_ADDED, migrateInLeader),
        new MigrationCost(REPLICA_LEADERS_TO_REMOVE, migrateOutLeader),
        new MigrationCost(CHANGED_REPLICAS, migrateReplicaNum));
  }

  public MigrationCost(String name, Map<Integer, Long> brokerCosts) {
    this.name = name;
    this.brokerCosts = brokerCosts;
  }

  // migrate out(leader)
  static Map<Integer, Long> recordSizeToFetch(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, true, (ignore) -> true, Replica::size);
  }

  // migrate in
  static Map<Integer, Long> recordSizeToSync(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, false, (ignore) -> true, Replica::size);
  }

  static Map<Integer, Long> replicaNumChanged(ClusterInfo before, ClusterInfo after) {
    return changedReplicaNumber(before, after);
  }

  static Map<Integer, Long> replicaLeaderToAdd(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, true, Replica::isLeader, ignore -> 1L);
  }

  static Map<Integer, Long> replicaLeaderToRemove(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, false, Replica::isLeader, ignore -> 1L);
  }

  /**
   * @param before the ClusterInfo before migrated replicas
   * @param after the ClusterInfo after migrated replicas
   * @param clusterBean cluster metrics
   * @return estimated migrated time required by all brokers (seconds)
   */
  public static Map<Integer, Long> brokerMigrationSecond(
      ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerInRate =
        Map.of(
            1,
                1.1748191951767201E9,
            2,
                1.1748191951767201E9,
            3,
                1.1748191951767201E9,
            4,
                1.1748191951767201E9,
            5,
                1.1748191951767201E9,
            6,
                1.1748191951767201E9);
    var brokerOutRate =
        Map.of(
            1,
            1.1748191951767201E9,
            2,
            1.1682146460995505E9,
            3,
            1.1489574188771384E9,
            4,
            1.1686907734783158E9,
            5,
            1.169250245562882E9,
            6,
            1.1691926483711882E9);

    /*
    var brokerInRate =
        before.nodes().stream()
            .collect(
                Collectors.toMap(
                    NodeInfo::id,
                    nodeInfo ->
                        brokerMaxRate(
                            nodeInfo.id(),
                            clusterBean,
                            PartitionMigrateTimeCost.MaxReplicationInRateBean.class)));
    var brokerOutRate =
        before.nodes().stream()
            .collect(
                Collectors.toMap(
                    NodeInfo::id,
                    nodeInfo ->
                        brokerMaxRate(
                            nodeInfo.id(),
                            clusterBean,
                            PartitionMigrateTimeCost.MaxReplicationOutRateBean.class)));

     */
    var brokerMigrateInSecond =
        MigrationCost.recordSizeToSync(before, after).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    brokerSize -> brokerSize.getValue() / brokerInRate.get(brokerSize.getKey())));
    var brokerMigrateOutSecond =
        MigrationCost.recordSizeToFetch(before, after).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    brokerSize -> brokerSize.getValue() / brokerOutRate.get(brokerSize.getKey())));
    brokerInRate.forEach((b, rate) -> System.out.println("broker: " + b + " inRate: " + rate));
    brokerOutRate.forEach((b, rate) -> System.out.println("broker: " + b + " OutRate: " + rate));
    return Stream.concat(before.nodes().stream(), after.nodes().stream())
        .map(NodeInfo::id)
        .distinct()
        .collect(
            Collectors.toMap(
                nodeId -> nodeId,
                nodeId ->
                    (long)
                        Math.max(
                            brokerMigrateInSecond.get(nodeId),
                            brokerMigrateOutSecond.get(nodeId))));
  }

  static OptionalDouble brokerMaxRate(
      int identity, ClusterBean clusterBean, Class<? extends HasBeanObject> statisticMetrics) {
    return clusterBean
        .brokerMetrics(identity, statisticMetrics)
        .mapToDouble(b -> ((HasMaxRate) b).maxRate())
        .max();
  }

  /**
   * @param before the ClusterInfo before migrated replicas
   * @param after the ClusterInfo after migrated replicas
   * @param migrateOut if data log need fetch from replica leader, set this true
   * @param predicate used to filter replicas
   * @param replicaFunction decide what information you want to calculate for the replica
   * @return the data size to migrated by all brokers
   */
  private static Map<Integer, Long> migratedChanged(
      ClusterInfo before,
      ClusterInfo after,
      boolean migrateOut,
      Predicate<Replica> predicate,
      Function<Replica, Long> replicaFunction) {
    var source = migrateOut ? after : before;
    var dest = migrateOut ? before : after;
    var changePartitions = ClusterInfo.findNonFulfilledAllocation(source, dest);

    if (migrateOut){
      System.out.println("out size: "  +changePartitions.stream()
              .flatMap(
                      p ->
                              dest.replicas(p).stream()
                                      .filter(predicate)
                                      .filter(r -> !source.replicas(p).contains(r)))
              .map(
                      r -> {
                        if (migrateOut) return dest.replicaLeader(r.topicPartition()).orElse(r);
                        return r;
                      })
              .toList().size());
    }else {
      System.out.println("in size: "  +changePartitions.stream()
              .flatMap(
                      p ->
                              dest.replicas(p).stream()
                                      .filter(predicate)
                                      .filter(r -> !source.replicas(p).contains(r)))
              .map(
                      r -> {
                        if (migrateOut) return dest.replicaLeader(r.topicPartition()).orElse(r);
                        return r;
                      })
              .toList().size());
    }


    var cost =
        changePartitions.stream()
            .flatMap(
                p ->
                    dest.replicas(p).stream()
                        .filter(predicate)
                        .filter(r -> !source.replicas(p).contains(r)))
            .map(
                r -> {
                  if (migrateOut) return dest.replicaLeader(r.topicPartition()).orElse(r);
                  return r;
                })
            .collect(
                Collectors.groupingBy(
                    r -> r.broker().id(),
                    Collectors.mapping(
                        Function.identity(), Collectors.summingLong(replicaFunction::apply))));
    return Stream.concat(dest.brokers().stream(), source.brokers().stream())
        .map(Broker::id)
        .distinct()
        .parallel()
        .collect(Collectors.toMap(Function.identity(), n -> cost.getOrDefault(n, 0L)));
  }

  private static Map<Integer, Long> changedReplicaNumber(ClusterInfo before, ClusterInfo after) {
    return Stream.concat(before.brokers().stream(), after.brokers().stream())
        .map(Broker::id)
        .distinct()
        .parallel()
        .collect(
            Collectors.toUnmodifiableMap(
                Function.identity(),
                id -> {
                  var removedLeaders =
                      before
                          .replicaStream(id)
                          .filter(
                              r ->
                                  after
                                      .replicaStream(r.topicPartitionReplica())
                                      .findAny()
                                      .isEmpty())
                          .count();
                  var newLeaders =
                      after
                          .replicaStream(id)
                          .filter(
                              r ->
                                  before
                                      .replicaStream(r.topicPartitionReplica())
                                      .findAny()
                                      .isEmpty())
                          .count();
                  return newLeaders - removedLeaders;
                }));
  }
}
