package org.astraea.workload;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OfflineLogProcessingApplication {

  public static class Producer implements Workload {

    @Override
    public void run(String bootstrapServer, String argument) {
      var shit = argument.split(":");
      var size = Integer.parseInt(shit[0]);

      try (var kafkaProducer = org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer()) {
        while (!Thread.currentThread().isInterrupted()) {
          final int day = LocalDateTime.now().toLocalDate().getDayOfYear();
          final int hour = LocalDateTime.now().getHour();
          var topicToSend = String.format("%s-%s-%s", shit[1], day, hour);
          var valueSize = (int) Math.abs(ThreadLocalRandom.current().nextGaussian() * size);
          kafkaProducer.send(new ProducerRecord<>(topicToSend, new byte[valueSize]));
          try {
            TimeUnit.MILLISECONDS.sleep((long) ThreadLocalRandom.current().nextGaussian() * 500L);
          } catch (InterruptedException e) {
            System.out.println("Thread has been interrupted");
            break;
          }
        }
      }
    }
  }

  public static class Consumer implements Workload {

    @Override
    public void run(String bootstrapServer, String argument) {
      var allArguments = argument.split(":");
      var groupName = allArguments[0];
      var targetTopic = allArguments[1];

      while (!Thread.currentThread().isInterrupted()) {
        try {
          final int day = LocalDateTime.now().toLocalDate().getDayOfYear();
          final int hour = (LocalDateTime.now().getHour() + 24 - 1) % 24;
          System.out.printf(
              "Schedule a offline processing task at %s%n", LocalDateTime.now().plusHours(1));
          System.out.printf(
              "Processing target %s%n", String.format("%s-%s-%s", targetTopic, day, hour));
          TimeUnit.HOURS.sleep(1);

          final KafkaConsumer<?, ?> kafkaConsumer =
              new KafkaConsumer<>(
                  Map.of(
                      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      bootstrapServer,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      groupName,
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "earliest"));
          kafkaConsumer.subscribe(Set.of(String.format("%s-%s-%s", targetTopic, day, hour)));
          for (int i = 0; i < 100000; i++) {
            kafkaConsumer.poll(Duration.ofSeconds(5));
          }
          kafkaConsumer.close();
        } catch (InterruptedException e) {
          System.out.println("Thread has been interrupted");
          break;
        }
      }
    }
  }
}
