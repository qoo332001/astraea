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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.BrokerPath;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;

public class BrokerDiskSpaceCost implements HasMoveCost {

  public static final String BROKER_COST_LIMIT_KEY = "max.broker.disk.space";
  public static final String DISK_COST_LIMIT_KEY = "max.disk.space";
  private final Configuration moveCostLimit;

  public BrokerDiskSpaceCost() {
    this.moveCostLimit =
        Configuration.of(
            Map.of(
                BROKER_COST_LIMIT_KEY,
                "2:500MB",
                DISK_COST_LIMIT_KEY,
                "0-/path1:100MB,0-/path2:200MB,1-/path1:300MB"));
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {

    var brokerCostLimit = brokerMoveCostLimit();
    var moveCost =
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .distinct()
            .parallel()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    id -> {
                      var beforeSize =
                          (Long)
                              before.replicaStream(id).map(Replica::size).mapToLong(y -> y).sum();
                      var addedSize =
                          (Long)
                              after
                                  .replicaStream(id)
                                  .filter(r -> before.replicaStream(id).noneMatch(r::equals))
                                  .map(Replica::size)
                                  .mapToLong(y -> y)
                                  .sum();
                      return beforeSize + addedSize;
                    }));

    var diskMoveCostLimit = diskMoveCostLimit();
    var beforeCost =
        before.brokerFolders().entrySet().stream()
            .flatMap(
                brokerPath ->
                    brokerPath.getValue().stream()
                        .map(
                            path ->
                                Map.entry(
                                    BrokerPath.of(brokerPath.getKey(), path),
                                    before
                                        .replicaStream(brokerPath.getKey())
                                        .filter(r -> r.path().equals(path))
                                        .mapToLong(Replica::size)
                                        .sum())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return null;
  }

  private Map<BrokerPath, DataSize> diskMoveCostLimit() {
    return this.moveCostLimit
        .string(DISK_COST_LIMIT_KEY)
        .map(
            s ->
                Arrays.stream(s.split(","))
                    .map(
                        idAndPath -> {
                          var brokerPathAndLimit = idAndPath.split(":");
                          var brokerPath = brokerPathAndLimit[0].split("-");
                          return Map.entry(
                              BrokerPath.of(Integer.parseInt(brokerPath[0]), brokerPath[1]),
                              DataSize.of(brokerPathAndLimit[1]));
                        })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .orElse(Map.of());
  }

  private Map<Integer, DataSize> brokerMoveCostLimit() {
    return this.moveCostLimit
        .string(BROKER_COST_LIMIT_KEY)
        .map(
            s ->
                Arrays.stream(s.split(","))
                    .map(
                        idAndPath -> {
                          var brokerAndLimit = idAndPath.split(":");
                          return Map.entry(
                              Integer.parseInt(brokerAndLimit[0]), DataSize.of(brokerAndLimit[1]));
                        })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .orElse(Map.of());
  }
}
