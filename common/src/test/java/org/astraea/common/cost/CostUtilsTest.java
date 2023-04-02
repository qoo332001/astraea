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
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CostUtilsTest {

  @Test
  void testChangedRecordSizeOverflow() {
    var limit = 1600;
    var totalResult =
        CostUtils.changedRecordSizeOverflow(
            beforeClusterInfo(), afterClusterInfo(), ignored -> true, limit);
    var overflowResult =
        CostUtils.changedRecordSizeOverflow(
            beforeClusterInfo(), afterClusterInfo(), ignored -> true, limit - 100);
    Assertions.assertFalse(totalResult);
    Assertions.assertTrue(overflowResult);
  }

  @Test
  void testBrokerDiskUsageSizeOverflow() {
    var limit =
        Map.of(
            0, DataSize.Byte.of(1600),
            1, DataSize.Byte.of(1598),
            2, DataSize.Byte.of(1600));
    var overFlowLimit =
        Map.of(
            0, DataSize.Byte.of(1600),
            1, DataSize.Byte.of(1598),
            2, DataSize.Byte.of(1500));
    var totalResult =
        CostUtils.brokerDiskUsageSizeOverflow(beforeClusterInfo(), afterClusterInfo(), limit);
    var overflowResult =
        CostUtils.brokerDiskUsageSizeOverflow(
            beforeClusterInfo(), afterClusterInfo(), overFlowLimit);
    Assertions.assertFalse(totalResult);
    Assertions.assertTrue(overflowResult);
  }

  @Test
  void testBrokerPathDiskUsageSizeOverflow() {
    var limit =
        Map.of(
            BrokerDiskSpaceCost.BrokerPath.of(0, "/path0"), DataSize.Byte.of(1600),
            BrokerDiskSpaceCost.BrokerPath.of(1, "/path0"), DataSize.Byte.of(1598),
            BrokerDiskSpaceCost.BrokerPath.of(2, "/path0"), DataSize.Byte.of(1600),
            BrokerDiskSpaceCost.BrokerPath.of(2, "/path1"), DataSize.Byte.of(600));
    var overFlowLimit =
        Map.of(
            BrokerDiskSpaceCost.BrokerPath.of(0, "/path0"), DataSize.Byte.of(1600),
            BrokerDiskSpaceCost.BrokerPath.of(1, "/path0"), DataSize.Byte.of(1598),
            BrokerDiskSpaceCost.BrokerPath.of(2, "/path0"), DataSize.Byte.of(1600),
            BrokerDiskSpaceCost.BrokerPath.of(2, "/path1"), DataSize.Byte.of(500));
    var totalResult =
        CostUtils.brokerPathDiskUsageSizeOverflow(beforeClusterInfo(), afterClusterInfo(), limit);
    var overflowResult =
        CostUtils.brokerPathDiskUsageSizeOverflow(
            beforeClusterInfo(), afterClusterInfo(), overFlowLimit);
    Assertions.assertFalse(totalResult);
    Assertions.assertTrue(overflowResult);
  }

  /*
  before distribution:
      p0: 0,1
      p1: 0,1
      p2: 2,0
  after distribution:
      p0: 2,1
      p1: 0,2
      p2: 1,0
  leader log size:
      p0: 100
      p1: 500
      p2  1000
   */
  private static ClusterInfo beforeClusterInfo() {
    var dataPath =
        Map.of(
            0,
            Map.of("/path0", new DescribeLogDirsResponse.LogDirInfo(null, Map.of())),
            1,
            Map.of("/path0", new DescribeLogDirsResponse.LogDirInfo(null, Map.of())),
            2,
            Map.of(
                "/path0",
                new DescribeLogDirsResponse.LogDirInfo(null, Map.of()),
                "/path1",
                new DescribeLogDirsResponse.LogDirInfo(null, Map.of())));
    var replicas =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(100)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(99)
                .isLeader(false)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(500)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(499)
                .isLeader(false)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(1000)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(1000)
                .isLeader(false)
                .path("/path0")
                .build());
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::nodeInfo)
            .distinct()
            .map(
                nodeInfo ->
                    Broker.of(
                        false,
                        new Node(nodeInfo.id(), "", nodeInfo.port()),
                        Map.of(),
                        dataPath.get(nodeInfo.id()),
                        List.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }

  private static ClusterInfo afterClusterInfo() {
    var dataPath =
        Map.of(
            0,
            Map.of("/path0", new DescribeLogDirsResponse.LogDirInfo(null, Map.of())),
            1,
            Map.of("/path0", new DescribeLogDirsResponse.LogDirInfo(null, Map.of())),
            2,
            Map.of(
                "/path0",
                new DescribeLogDirsResponse.LogDirInfo(null, Map.of()),
                "/path1",
                new DescribeLogDirsResponse.LogDirInfo(null, Map.of())));
    var replicas =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(100)
                .isLeader(true)
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(99)
                .isLeader(false)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(500)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(500)
                .isLeader(false)
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(1000)
                .isLeader(true)
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(1000)
                .isLeader(false)
                .path("/path0")
                .build());
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::nodeInfo)
            .distinct()
            .map(
                nodeInfo ->
                    Broker.of(
                        false,
                        new Node(nodeInfo.id(), "", nodeInfo.port()),
                        Map.of(),
                        dataPath.get(nodeInfo.id()),
                        List.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }
}
