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

import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

/**
 * PartitionCost: more replica log size -> higher partition score BrokerCost: more replica log size
 * in broker -> higher broker cost ClusterCost: The more unbalanced the replica log size among
 * brokers -> higher cluster cost MoveCost: more replicas log size migrate
 */
public class ReplicaLeaderSizeOutCost implements HasMoveCost {
  private final Dispersion dispersion = Dispersion.cov();

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var changePartitions = ClusterInfo.findNonFulfilledAllocation(before, after);
    var cost =
        changePartitions.stream()
            .flatMap(p -> before.replicas(p).stream().filter(r -> !after.replicas(p).contains(r)))
            .map(x -> before.replicaLeader(x.topicPartition()).get())
            .collect(
                Collectors.groupingBy(
                    r -> r.nodeInfo().id(),
                    Collectors.mapping(Replica::size, Collectors.summingLong(x -> x))))
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, x -> DataSize.Byte.of(x.getValue())));
    var result =
        before.nodes().stream()
            .map(n -> Map.entry(n.id(), cost.getOrDefault(n.id(), DataSize.ZERO)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return MoveCost.movedReplicaLeaderOutSize(result);
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the used space for each broker
   */
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicas().stream()
            .collect(
                Collectors.groupingBy(
                    r -> r.nodeInfo().id(),
                    Collectors.mapping(
                        r ->
                            clusterInfo
                                .replicaLeader(r.topicPartition())
                                .map(Replica::size)
                                .orElseThrow(),
                        Collectors.summingDouble(x -> x))));
    return () -> result;
  }

  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerCost.values());
    return () -> value;
  }

  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicaLeaders().stream()
            .collect(
                Collectors.toMap(
                    Replica::topicPartition,
                    r ->
                        (double)
                            clusterInfo
                                .replicaLeader(r.topicPartition())
                                .map(Replica::size)
                                .orElseThrow()));
    return () -> result;
  }
}
