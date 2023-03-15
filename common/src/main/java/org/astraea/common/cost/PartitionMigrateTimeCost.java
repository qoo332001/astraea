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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.broker.HasMeter;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.stats.Debounce;
import org.astraea.common.metrics.stats.Max;

/** MoveCost: more max write rate change -> higher migrate cost. */
public class PartitionMigrateTimeCost implements HasMoveCost {
  private static final String REPLICATION_IN_RATE = "replication_in_rate";
  private static final String REPLICATION_OUT_RATE = "replication_out_rate";
  private static final Duration DEFAULT_DURATION = Duration.ofMinutes(100);
  private final Duration duration;
  static final Map<Integer, Double> lastInRecord = new HashMap<>();
  static final Map<Integer, Double> lastOutRecord = new HashMap<>();
  static final Map<Integer, Duration> lastInTime = new HashMap<>();
  static final Map<Integer, Duration> lastOutTime = new HashMap<>();

  static final Map<Integer, Debounce<Double>> inDenounces = new HashMap<>();
  static final Map<Integer, Debounce<Double>> outDenounces = new HashMap<>();
  static final Map<Integer, Sensor<Double>> maxBrokerReplicationInRate = new HashMap<>();
  static final Map<Integer, Sensor<Double>> maxBrokerReplicationOutRate = new HashMap<>();
  // metrics windows size
  static final int rateDurationSec = 120;

  public PartitionMigrateTimeCost() {
    this.duration = DEFAULT_DURATION;
  }

  public PartitionMigrateTimeCost(Duration duration) {
    this.duration = duration;
  }

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  @Override
  public Optional<Fetcher> fetcher() {
    return Fetcher.of(
        List.of(
            client -> List.of(ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.fetch(client)),
            client ->
                List.of(ServerMetrics.BrokerTopic.REPLICATION_BYTES_OUT_PER_SEC.fetch(client))));
  }

  @Override
  public Collection<MetricSensor> sensors() {
    return List.of(
        (identity, beans) ->
            Map.of(
                identity,
                beans.stream()
                    .filter(b -> b instanceof ServerMetrics.BrokerTopic.Meter)
                    .map(b -> (ServerMetrics.BrokerTopic.Meter) b)
                    .filter(
                        g ->
                            g.type() == ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC
                                || g.type()
                                    == ServerMetrics.BrokerTopic.REPLICATION_BYTES_OUT_PER_SEC)
                    .map(
                        g -> {
                          var maxRateSensor =
                              g.metricsName()
                                      .equals(
                                          ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC
                                              .metricName())
                                  ? maxBrokerReplicationInRate.computeIfAbsent(
                                      identity,
                                      ignore ->
                                          Sensor.builder()
                                              .addStat(REPLICATION_IN_RATE, Max.<Double>of())
                                              .build())
                                  : maxBrokerReplicationOutRate.computeIfAbsent(
                                      identity,
                                      ignore ->
                                          Sensor.builder()
                                              .addStat(REPLICATION_OUT_RATE, Max.<Double>of())
                                              .build());
                          maxRateSensor.record(g.oneMinuteRate());
                          if (g.metricsName()
                              .equals(
                                  ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC
                                      .metricName())) {
                            return (MaxReplicationInRateBean)
                                () ->
                                    new BeanObject(
                                        g.beanObject().domainName(),
                                        g.beanObject().properties(),
                                        Map.of(
                                            HasRate.ONE_MIN_RATE_KEY,
                                            maxRateSensor.measure(REPLICATION_IN_RATE)),
                                        System.currentTimeMillis());
                          } else
                            return (MaxReplicationOutRateBean)
                                () ->
                                    new BeanObject(
                                        g.beanObject().domainName(),
                                        g.beanObject().properties(),
                                        Map.of(
                                            HasRate.ONE_MIN_RATE_KEY,
                                            maxRateSensor.measure(REPLICATION_OUT_RATE)),
                                        System.currentTimeMillis());
                        })
                    /*
                    .map(
                        g -> {
                          var current = Duration.ofMillis(System.currentTimeMillis());
                          var newCount = Double.valueOf(g.count());
                          var maxRateSensor =
                              g.metricsName()
                                      .equals(
                                          ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC
                                              .metricName())
                                  ? maxBrokerReplicationInRate.computeIfAbsent(
                                      identity,
                                      ignore ->
                                          Sensor.builder()
                                              .addStat(REPLICATION_IN_RATE, Max.<Double>of())
                                              .build())
                                  : maxBrokerReplicationOutRate.computeIfAbsent(
                                      identity,
                                      ignore ->
                                          Sensor.builder()
                                              .addStat(REPLICATION_OUT_RATE, Max.<Double>of())
                                              .build());
                          if (g.metricsName()
                              .equals(
                                  ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC
                                      .metricName())) {
                            var debounce =
                                inDenounces.computeIfAbsent(
                                    identity,
                                    ignore -> Debounce.of(Duration.ofSeconds(rateDurationSec)));
                            debounce
                                .record(newCount, current.toMillis())
                                .ifPresent(
                                    debouncedValue -> {
                                      if (lastInTime.containsKey(identity))
                                        maxRateSensor.record(
                                            (debouncedValue
                                                    - lastInRecord.getOrDefault(identity, 0.0))
                                                / (current.getSeconds()
                                                    - lastInTime.get(identity).getSeconds()));
                                      lastInTime.put(identity, current);
                                      lastInRecord.put(identity, debouncedValue);
                                    });
                            return (MaxReplicationInRateBean)
                                () ->
                                    new BeanObject(
                                        g.beanObject().domainName(),
                                        g.beanObject().properties(),
                                        Map.of(
                                            HasRate.ONE_MIN_RATE_KEY,
                                            maxRateSensor.measure(REPLICATION_IN_RATE)),
                                        System.currentTimeMillis());
                          } else {
                            var debounce =
                                outDenounces.computeIfAbsent(
                                    identity,
                                    ignore -> Debounce.of(Duration.ofSeconds(rateDurationSec)));
                            debounce
                                .record(newCount, current.toMillis())
                                .ifPresent(
                                    debouncedValue -> {
                                      if (lastOutTime.containsKey(identity))
                                        maxRateSensor.record(
                                            ((debouncedValue
                                                    - lastOutRecord.getOrDefault(identity, 0.0))
                                                / (current.getSeconds()
                                                    - lastOutTime.get(identity).getSeconds())));
                                      lastOutTime.put(identity, current);
                                      lastOutRecord.put(identity, debouncedValue);
                                    });
                            return (MaxReplicationOutRateBean)
                                () ->
                                    new BeanObject(
                                        g.beanObject().domainName(),
                                        g.beanObject().properties(),
                                        Map.of(
                                            HasRate.ONE_MIN_RATE_KEY,
                                            maxRateSensor.measure(REPLICATION_OUT_RATE)),
                                        System.currentTimeMillis());
                          }
                        })
                         */
                    .collect(Collectors.toList())));
  }

  public Map<Integer, Double> brokerMaxRate(
      ClusterInfo clusterInfo,
      ClusterBean clusterBean,
      Class<? extends HasBeanObject> statisticMetrics,
      String metricName) {
    var usedBandwidth =
        clusterBean.all().getOrDefault(0, List.of()).stream()
            .filter(x -> ServerMetrics.BrokerTopic.Meter.class.isAssignableFrom(x.getClass()))
            .filter(
                x ->
                    x.metricName()
                        .equals(
                            ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.metricName()))
            .max(Comparator.comparing(HasBeanObject::createdTimestamp))
            .map(x -> (ServerMetrics.BrokerTopic.Meter) x)
            .get()
            .oneMinuteRate();
    var maxReplicationBandwidth =
        clusterInfo.brokers().stream()
            .map(
                broker ->
                    Map.entry(
                        broker.id(),
                        clusterBean.all().getOrDefault(broker.id(), List.of()).stream()
                            .filter(x -> statisticMetrics.isAssignableFrom(x.getClass()))
                            .mapToDouble(
                                x -> {
                                  return ((HasMeter) x).oneMinuteRate();
                                })
                            .max()
                            .orElseThrow(
                                () ->
                                    new NoSufficientMetricsException(
                                        this,
                                        Duration.ofSeconds(1),
                                        "No metric for broker" + broker.id()))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return maxReplicationBandwidth;
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerInRate =
        brokerMaxRate(
            before,
            clusterBean,
            MaxReplicationInRateBean.class,
            ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.metricName());
    var brokerOutRate =
        brokerMaxRate(
            before,
            clusterBean,
            MaxReplicationOutRateBean.class,
            ServerMetrics.BrokerTopic.REPLICATION_BYTES_OUT_PER_SEC.metricName());
    var needToMigrateIn =
        new ReplicaLeaderSizeInCost()
            .moveCost(before, after, clusterBean)
            .movedReplicaLeaderInSize();
    var needToMigrateOut =
        new ReplicaLeaderSizeOutCost()
            .moveCost(before, after, clusterBean)
            .movedReplicaLeaderOutSize();

    var brokerMigrateInTime =
        needToMigrateIn.entrySet().stream()
            .map(
                brokerSize ->
                    Map.entry(
                        brokerSize.getKey(),
                        brokerSize.getValue().bytes() / brokerInRate.get(brokerSize.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var brokerMigrateOutTime =
        needToMigrateOut.entrySet().stream()
            .map(
                brokerSize ->
                    Map.entry(
                        brokerSize.getKey(),
                        brokerSize.getValue().bytes() / brokerOutRate.get(brokerSize.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var result =
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .distinct()
            .map(
                nodeInfo ->
                    Map.entry(
                        nodeInfo.id(),
                        Math.max(
                            brokerMigrateInTime.get(nodeInfo.id()),
                            brokerMigrateOutTime.get(nodeInfo.id()))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    System.out.println("------------------------------------------");
    brokerInRate.forEach(
        (broker, rate) -> System.out.println("brokerIn" + broker + " rate:" + rate));
    brokerOutRate.forEach(
        (broker, rate) -> System.out.println("brokerOut" + broker + " rate:" + rate));
    return MoveCost.brokerMigrateTime(result);
  }

  public interface MaxReplicationInRateBean extends HasMeter {}

  public interface MaxReplicationOutRateBean extends HasMeter {}
}
