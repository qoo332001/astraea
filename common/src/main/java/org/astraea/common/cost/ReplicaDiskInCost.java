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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.SensorBuilder;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricSensors;
import org.astraea.common.metrics.stats.Avg;

public class ReplicaDiskInCost implements HasClusterCost, HasBrokerCost {
  private final Dispersion dispersion = Dispersion.correlationCoefficient();

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerCost.values());
    return () -> value;
  }

  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var partitionCost = partitionCost(clusterInfo, clusterBean);
    var brokerLoad =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        partitionCost.apply(node.id()).values().stream()
                            .mapToDouble(rate -> rate)
                            .sum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return () -> brokerLoad;
  }

  private Function<Integer, Map<TopicPartition, Double>> partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var replicaIn = clusterBean.statisticsByReplica(LogMetrics.Log.SIZE.metricName(), "avg");
    var scoreForBroker =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        replicaIn.entrySet().stream()
                            .filter(x -> x.getKey().brokerId() == node.id())
                            .collect(
                                Collectors.groupingBy(
                                    x ->
                                        TopicPartition.of(
                                            x.getKey().topic(), x.getKey().partition())))
                            .entrySet()
                            .stream()
                            .map(
                                entry ->
                                    Map.entry(
                                        entry.getKey(),
                                        entry.getValue().stream()
                                            .mapToDouble(Map.Entry::getValue)
                                            .max()
                                            .orElseThrow()))
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return scoreForBroker::get;
  }

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  @Override
  public Collection<MetricSensors<?>> sensors() {
    return List.of(
        new MetricSensors<TopicPartitionReplica>() {
          final Map<TopicPartitionReplica, Sensor<Double>> sensors = new HashMap<>();

          @Override
          public Class<? extends HasBeanObject> metricClass() {
            return LogMetrics.Log.Gauge.class;
          }

          @Override
          public void record(int identity, Collection<? extends HasBeanObject> beans) {
            beans.forEach(
                bean -> {
                  var metricName = bean.beanObject().properties().get("name");
                  // && metricName.equals(metricClass().toString()))
                  if (metricName != null)
                    if (bean.beanObject().domainName().equals(LogMetrics.DOMAIN_NAME)
                        && bean.beanObject().properties().get("type").equals(LogMetrics.LOG_TYPE))
                      sensors
                          .get(
                              TopicPartitionReplica.of(
                                  bean.beanObject().properties().get("topic"),
                                  Integer.parseInt(bean.beanObject().properties().get("partition")),
                                  identity))
                          .record(
                              Double.valueOf(
                                  bean.beanObject().attributes().get("Value").toString()));
                });
          }

          @Override
          public Map<TopicPartitionReplica, Sensor<Double>> sensors() {
            return this.sensors;
          }

          @Override
          public Sensor<Double> sensor(TopicPartitionReplica key) {
            return sensors.get(key);
          }

          @Override
          public void addSensorKey(List<?> e) {
            e.forEach(
                tpr ->
                    sensors.put(
                        (TopicPartitionReplica) tpr,
                        new SensorBuilder<Double>()
                            .addStat(Avg.EXP_WEIGHT_BY_TIME_KEY, Avg.expWeightByTime(Duration.ofSeconds(1)))
                            .build()));
          }
        });
  }
}
