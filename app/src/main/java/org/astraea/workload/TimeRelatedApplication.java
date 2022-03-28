package org.astraea.workload;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.consumer.Record;
import org.astraea.utils.DataSize;

public class TimeRelatedApplication {

  public static class Producer implements Workload {

    public double positiveGaussian() {
      return Math.abs(ThreadLocalRandom.current().nextGaussian());
    }

    @Override
    public void run(String bootstrapServer, String argument) throws InterruptedException {
      final String[] allArguments = argument.split(",");
      final var payloadSize = new DataSize.Field().convert(allArguments[0]).bits().longValue() / 8;
      final String[] topicName = allArguments[1].split(":");

      try (var kafkaProducer = org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer()) {
        while (!Thread.currentThread().isInterrupted()) {
          var topicToSend = topicName[LocalDateTime.now().getMinute() / 10 % topicName.length];
          var valueSize = (int) (positiveGaussian() * payloadSize);
          kafkaProducer.send(new ProducerRecord<>(topicToSend, new byte[valueSize]));
          TimeUnit.MILLISECONDS.sleep((long) positiveGaussian() * 100L);
        }
      }
    }

    @Override
    public String explainArgument() {
      return "(data size),[topic 1]:[topic 2]:[topic 3]...";
    }
  }

  public static class Consumer implements Workload {

    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] all = argument.split(",");
      final String[] topicName = all[1].split(":");

      try (var build =
          org.astraea.consumer.Consumer.builder()
              .brokers(bootstrapServer)
              .topics(Set.of(topicName))
              .groupId(all[0])
              .build()) {
        while (!Thread.currentThread().isInterrupted()) {
          final Collection<Record<byte[], byte[]>> poll = build.poll(Duration.ofSeconds(1));
        }
      }
    }

    @Override
    public String explainArgument() {
      return "(consumer group name),[topic 1]:[topic 2]:[topic 3]...";
    }
  }
}
