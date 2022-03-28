package org.astraea.workload;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.astraea.producer.Serializer;
import org.astraea.topic.TopicAdmin;
import org.astraea.utils.DataSize;

public class HotKeyApplication {

  public static class Producer implements Workload {
    @Override
    public void run(String bootstrapServer, String argument) throws InterruptedException {

      Supplier<Double> positiveGaussian =
          () -> Math.abs(ThreadLocalRandom.current().nextGaussian());

      String[] split = argument.split(":");
      var topicName = split[0];
      var partitionCount = Integer.parseInt(split[1]);
      var replicaCount = Integer.parseInt(split[2]);
      var retentionTime = split[3];
      var totalOutput = new DataSize.Field().convert(split[4]).bits().longValue() / 8;

      TopicAdmin.of(bootstrapServer)
          .creator()
          .topic(topicName)
          .numberOfPartitions(partitionCount)
          .numberOfReplicas((short) replicaCount)
          .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, retentionTime))
          .create();

      // generate the weight for each partition
      var weightOfEachPartition =
          IntStream.range(0, partitionCount)
              .mapToObj(x -> (int) Math.ceil(positiveGaussian.get()) * 2)
              .collect(Collectors.toList());

      var totalWeight = weightOfEachPartition.stream().mapToInt(x -> x).sum();
      var sizeOfEachChunk = totalOutput / totalWeight;

      try (var producer =
          org.astraea.producer.Producer.builder()
              .brokers(bootstrapServer)
              .keySerializer(Serializer.LONG)
              .build()
              .kafkaProducer()) {
        while (!Thread.interrupted()) {
          IntStream.range(0, partitionCount)
              .parallel()
              .forEach(
                  partition -> {
                    var record =
                        new ProducerRecord<>(
                            topicName, (long) partition, new byte[(int) sizeOfEachChunk]);
                    IntStream.range(0, weightOfEachPartition.get(partition))
                        .forEach(x -> producer.send(record));
                  });

          TimeUnit.MILLISECONDS.sleep((long) (positiveGaussian.get() * 200));
        }
      }
    }

    @Override
    public String explainArgument() {
      return "(topic name):(partition count):(replica):(retention time):(output data size)";
    }
  }

  public static class Consumer implements Workload {
    @Override
    public void run(String bootstrapServer, String argument) throws InterruptedException {
      String[] split = argument.split(":");
      var topicName = split[0];
      var groupId = split[1];

      TimeUnit.SECONDS.sleep(3);

      try (var consumer =
          org.astraea.consumer.Consumer.builder()
              .brokers(bootstrapServer)
              .groupId(groupId)
              .topics(Set.of(topicName))
              .build()) {
        while (!Thread.interrupted()) {
          consumer.poll(Duration.ofSeconds(3));
        }
      }
    }

    @Override
    public String explainArgument() {
      return "(topic name):(group id)";
    }
  }
}
