package org.astraea.workload;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.consumer.Record;
import org.astraea.utils.DataUnit;

public class TimeRelatedApplication {

  public static class Producer implements Workload {

    @Override
    public void run(String bootstrapServer, String argument) throws InterruptedException {
      final String[] allArguments = argument.split(",");
      final int payloadSize = Integer.parseInt(allArguments[0]);
      final String[] topicName = allArguments[1].split(":");

      try (var kafkaProducer = org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer()) {
        while (!Thread.currentThread().isInterrupted()) {
          var topicToSend = topicName[LocalDateTime.now().getMinute() / 10 % topicName.length];
          var valueSize =
              (int) ((ThreadLocalRandom.current().nextGaussian() + 1) / 2 * payloadSize);
          kafkaProducer.send(new ProducerRecord<>(topicToSend, new byte[valueSize]));
          System.out.printf("Send %s to %s%n", DataUnit.Byte.of(valueSize), topicToSend);
          TimeUnit.MILLISECONDS.sleep(
              (long) (ThreadLocalRandom.current().nextGaussian() + 1) / 2 * 100L);
        }
      }
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
  }
}
