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
package org.astraea.app.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.kafka.HasValue;
import org.astraea.app.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaSizeCostTest {

  @Test
  void partitionCost() {
    var brokerDiskSize = Map.of(1, 1000, 2, 1000, 3, 1000);
    var loadCostFunction = new ReplicaSizeCost(brokerDiskSize);
    var broker1ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(1);
    var broker2ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(2);
    var broker3ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(3);
    // broker1
    Assertions.assertEquals(0.85, broker1ReplicaLoad.get(new TopicPartition("test-1", 0)));
    Assertions.assertEquals(0.45, broker1ReplicaLoad.get(new TopicPartition("test-1", 1)));
    Assertions.assertEquals(0.35, broker1ReplicaLoad.get(new TopicPartition("test-2", 1)));
    // broker2
    Assertions.assertEquals(0.45, broker2ReplicaLoad.get(new TopicPartition("test-1", 1)));
    Assertions.assertEquals(0, broker2ReplicaLoad.get(new TopicPartition("test-2", 0)));
    // broker3
    Assertions.assertEquals(0.85, broker3ReplicaLoad.get(new TopicPartition("test-1", 0)));
    Assertions.assertEquals(0, broker3ReplicaLoad.get(new TopicPartition("test-2", 0)));
    Assertions.assertEquals(0.35, broker3ReplicaLoad.get(new TopicPartition("test-2", 1)));
  }

  @Test
  void brokerCost() {
    var brokerDiskSize = Map.of(1, 1000, 2, 1000, 3, 1000);
    var loadCostFunction = new ReplicaSizeCost(brokerDiskSize);
    var brokerReplicaLoad = loadCostFunction.brokerCost(exampleClusterInfo()).value();
    Assertions.assertEquals(brokerReplicaLoad.get(1), 0.85 + 0.45 + 0.35);
    Assertions.assertEquals(brokerReplicaLoad.get(2), 0.45);
    Assertions.assertEquals(brokerReplicaLoad.get(3), 0.85 + 0.35);
  }

  private ClusterInfo exampleClusterInfo() {
    var sizeTP1_0 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 891289600);
    var sizeTP1_1 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 471859200);
    var sizeTP2_0 =
        fakeBeanObject("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 0);
    var sizeTP2_1 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "1", 367001600);
    Collection<HasBeanObject> broker1 = List.of(sizeTP1_0, sizeTP1_1, sizeTP2_1);
    Collection<HasBeanObject> broker2 = List.of(sizeTP1_1, sizeTP2_0);
    Collection<HasBeanObject> broker3 = List.of(sizeTP2_1, sizeTP1_0, sizeTP2_0);
    return new FakeClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1));
      }

      @Override
      public Set<String> topics() {
        return Set.of("test-1", "test-2");
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        if (topic.equals("test-1"))
          return List.of(
              ReplicaInfo.of("test-1", 0, NodeInfo.of(1, "", -1), true, true, false),
              ReplicaInfo.of("test-1", 0, NodeInfo.of(3, "", -1), false, true, false),
              ReplicaInfo.of("test-1", 1, NodeInfo.of(1, "", -1), false, true, false),
              ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false));
        else
          return List.of(
              ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), false, true, false),
              ReplicaInfo.of("test-2", 0, NodeInfo.of(3, "", -1), true, true, false),
              ReplicaInfo.of("test-2", 1, NodeInfo.of(1, "", -1), false, true, false),
              ReplicaInfo.of("test-2", 1, NodeInfo.of(3, "", -1), true, true, false));
      }

      @Override
      public ClusterBean clusterBean() {
        return ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
      }
    };
  }

  private HasValue fakeBeanObject(
      String type, String name, String topic, String partition, long size) {
    BeanObject beanObject =
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size));
    return HasValue.of(beanObject);
  }
}