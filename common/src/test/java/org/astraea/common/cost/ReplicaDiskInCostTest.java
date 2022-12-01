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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.metrics.SensorBuilder;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.metrics.stats.Avg;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaDiskInCostTest extends RequireBrokerCluster {

  @Test
  void testBrokerCost() {
    var loadCostFunction = new ReplicaDiskInCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals((100.0 + 1000) / 2, brokerLoad.get(0));
    Assertions.assertEquals((100.0 + 200) / 2, brokerLoad.get(1));
    Assertions.assertEquals((10.0 + 20) / 2, brokerLoad.get(2));
  }

  @Test
  void testClusterCost() {
    final Dispersion dispersion = Dispersion.correlationCoefficient();
    var loadCostFunction = new ReplicaDiskInCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    var clusterCost = loadCostFunction.clusterCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(dispersion.calculate(brokerLoad.values()), clusterCost);
  }

  private ClusterInfo<Replica> clusterInfo() {
    return ClusterInfo.of(
        Set.of(NodeInfo.of(0, "", -1), NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1)), List.of());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static ClusterBean clusterBean() {
    var topicName = Utils.randomString(10);
    var sensors =
        List.of(
            new SensorBuilder().addStat(Avg.AVG_KEY, Avg.of()).build(),
            new SensorBuilder().addStat(Avg.AVG_KEY, Avg.of()).build(),
            new SensorBuilder().addStat(Avg.AVG_KEY, Avg.of()).build());
    sensors.get(0).record(100.0);
    sensors.get(0).record(1000.0);
    sensors.get(1).record(100.0);
    sensors.get(1).record(200.0);
    sensors.get(2).record(10.0);
    sensors.get(2).record(20.0);
    return ClusterBean.of(
        Map.of(),
        Map.of(
            LogMetrics.Log.SIZE.metricName(),
            Map.of(
                TopicPartitionReplica.of(topicName, 0, 0), sensors.get(0),
                TopicPartitionReplica.of(topicName, 1, 1), sensors.get(1),
                TopicPartitionReplica.of(topicName, 2, 2), sensors.get(2))));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  void testFetcher() throws InterruptedException {
    var interval = Duration.ofMillis(300);
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      try (var collector = MetricCollector.builder().interval(interval).build()) {
        var costFunction = new ReplicaDiskInCost();
        // create come partition to get metrics
        admin
            .creator()
            .topic(topicName)
            .numberOfPartitions(4)
            .numberOfReplicas((short) 1)
            .run()
            .toCompletableFuture()
            .get();
        var producer = Producer.of(bootstrapServers());
        producer
            .send(Record.builder().topic(topicName).partition(0).key(new byte[100]).build())
            .toCompletableFuture()
            .join();
        collector.addFetcher(
            costFunction.fetcher().orElseThrow(), (id, err) -> Assertions.fail(err.getMessage()));
        collector.registerLocalJmx(0);
        var tpr =
            List.of(
                TopicPartitionReplica.of(topicName, 0, 0),
                TopicPartitionReplica.of(topicName, 1, 0),
                TopicPartitionReplica.of(topicName, 2, 0),
                TopicPartitionReplica.of(topicName, 3, 0));
        collector.addSensors(costFunction.sensors());
        collector.sensor(LogMetrics.Log.Gauge.class).addSensorKey(tpr);
        Utils.sleep(interval);
        Assertions.assertEquals(
            170.0,
            collector
                .sensor(LogMetrics.Log.Gauge.class)
                .sensor(TopicPartitionReplica.of(topicName, 0, 0))
                .measure(Avg.AVG_KEY));
        Assertions.assertEquals(4, collector.sensor(LogMetrics.Log.Gauge.class).sensors().size());
        var currentMetricsNum = collector.clusterBean().all().get(0).size();
        Assertions.assertFalse(collector.listIdentities().isEmpty());
        Assertions.assertFalse(collector.listFetchers().isEmpty());

        // wait for next fetch
        Utils.sleep(interval);
        Assertions.assertTrue(collector.clusterBean().all().get(0).size() > currentMetricsNum);
        Assertions.assertEquals(
            170.0,
            collector
                .sensor(LogMetrics.Log.Gauge.class)
                .sensor(TopicPartitionReplica.of(topicName, 0, 0))
                .measure(Avg.AVG_KEY));

      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
