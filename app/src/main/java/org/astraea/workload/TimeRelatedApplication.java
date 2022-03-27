package org.astraea.workload;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Record;
import org.astraea.producer.Producer;
import org.astraea.utils.DataSize;
import org.astraea.utils.DataUnit;

public class TimeRelatedApplication {

  public static class Producer implements Workload {
    /**
     * @param bootstrapServer
     * @param argument "payloadSize,topic1:topic2:topic3:topic4"
     */
    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] all = argument.split(",");
      final int payloadSize = Integer.parseInt(all[0]);
      final String[] topicName = all[1].split(":");
      final KafkaProducer<byte[], byte[]> kafkaProducer =
          org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();

      while (!Thread.currentThread().isInterrupted()) {
        var topicToSend = topicName[LocalDateTime.now().getMinute() / 10 % topicName.length];
        var valueSize = (int) Math.abs(ThreadLocalRandom.current().nextGaussian() * payloadSize);
        kafkaProducer.send(new ProducerRecord<>(topicToSend, new byte[valueSize]));
        System.out.printf("Send %s to %s%n", DataUnit.Byte.of(valueSize), topicToSend);
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
     * @param argument "groupId,topic1:topic2:topic3:topic4"
     */
    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] all = argument.split(",");
      final String[] topicName = all[1].split(":");

      final org.astraea.consumer.Consumer<byte[], byte[]> build =
          org.astraea.consumer.Consumer.builder()
              .brokers(bootstrapServer)
              .topics(Set.of(topicName))
              .groupId(all[0])
              .build();

      while (!Thread.currentThread().isInterrupted()) {
        try {
          final Collection<Record<byte[], byte[]>> poll = build.poll(Duration.ofSeconds(1));
          final DataSize of = DataUnit.Byte.of(poll.stream().mapToInt(x -> x.value().length).sum());
          System.out.printf("consume %s%n", of);
        } catch (Exception e) {
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

    final Thread apply =
        doIt(new Producer())
            .apply(
                "192.168.103.177:10001,192.168.103.177:10002,192.168.103.177:10003",
                "100000,topicAAA:topicBBB:topicCCC:topicDDD:topicEEE");
    final Thread apply1 =
        doIt(new Consumer())
            .apply(
                "192.168.103.177:10001,192.168.103.177:10002,192.168.103.177:10003",
                "fucking_group,topicAAA:topicBBB:topicCCC:topicDDD:topicEEE");

    apply.start();
    apply1.start();

    TimeUnit.SECONDS.sleep(1);

    apply.interrupt();
    apply1.interrupt();

    apply.join();
    apply1.join();
  }
}
