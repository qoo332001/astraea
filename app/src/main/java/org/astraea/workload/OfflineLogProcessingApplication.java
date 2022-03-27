package org.astraea.workload;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.consumer.Consumer;
import org.astraea.utils.DataRate;
import org.astraea.utils.DataUnit;

public class OfflineLogProcessingApplication {

  public static class Producer implements Workload {

    public AtomicLong acc = new AtomicLong();

    /**
     * @param bootstrapServer
     * @param argument "payloadSize:topic"
     */
    @Override
    public void run(String bootstrapServer, String argument) {
      var shit = argument.split(":");
      var size = Integer.parseInt(shit[0]);
      final KafkaProducer<byte[], byte[]> kafkaProducer =
          org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();

      while (!Thread.currentThread().isInterrupted()) {
        final int day = LocalDateTime.now().toLocalDate().getDayOfYear();
        final int hour = LocalDateTime.now().getHour();
        var topicToSend = String.format("%s-%s-%s", shit[1], day, hour);
        var valueSize = (int) Math.abs(ThreadLocalRandom.current().nextGaussian() * size);
        kafkaProducer.send(new ProducerRecord<>(topicToSend, new byte[valueSize]));
        acc.addAndGet(valueSize);
        // System.out.printf("Send %s to %s%n", DataUnit.Byte.of(valueSize), topicToSend);
        try {
          TimeUnit.MILLISECONDS.sleep((long) ThreadLocalRandom.current().nextGaussian() * 500L);
        } catch (InterruptedException e) {
          System.out.println("Thread has been interrupted");
          break;
        }
      }
    }
  }

  public static class Consumer implements Workload {

    /**
     * @param bootstrapServer
     * @param argument groupId:topicName
     */
    @Override
    public void run(String bootstrapServer, String argument) {
      var shits = argument.split(":");

      while (!Thread.currentThread().isInterrupted()) {
        try {
          final int day = LocalDateTime.now().toLocalDate().getDayOfYear();
          final int hour = (LocalDateTime.now().getHour() + 24 - 1) % 24;
          System.out.printf(
              "Schedule a offline processing task at %s%n", LocalDateTime.now().plusHours(1));
          System.out.printf(
              "Processing target %s%n", String.format("%s-%s-%s", shits[1], day, hour));
          TimeUnit.HOURS.sleep(1);

          final KafkaConsumer<?, ?> kafkaConsumer =
              new KafkaConsumer<>(
                  Map.of(
                      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      bootstrapServer,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      shits[0],
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "latest"));
          kafkaConsumer.subscribe(Set.of(String.format("%s-%s-%s", shits[1], day, hour)));
        } catch (InterruptedException e) {
          System.out.println("Thread has been interrupted");
          break;
        }
      }
    }
  }

  public static BiFunction<String, String, Thread> doIt(Workload workload) {
    return (String x1, String x2) ->
        new Thread(
            () -> {
              workload.run(x1, x2);
            });
  }

  public static void main(String[] args) throws InterruptedException {

    final Producer producer = new Producer();

    final Thread apply =
        doIt(producer)
            .apply(
                "192.168.103.177:10001,192.168.103.177:10002,192.168.103.177:10003",
                "1000:offline-log-processing-01");
    final Thread apply1 =
        doIt(new Consumer())
            .apply(
                "192.168.103.177:10001,192.168.103.177:10002,192.168.103.177:10003",
                "fucking_group:offline-log-processing-01");

    new Thread(
            () -> {
              long last = producer.acc.get();
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  break;
                }
                long now = producer.acc.get();
                System.out.printf(
                    "Producer data rate %s%n",
                    DataRate.of(now - last, DataUnit.Byte, Duration.ofSeconds(10)));
                last = now;
              }
            })
        .start();

    apply.start();
    apply1.start();

    apply.join();
    apply1.join();
  }
}
