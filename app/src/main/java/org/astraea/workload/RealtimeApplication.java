package org.astraea.workload;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RealtimeApplication {
  public static class Producer implements Workload {
    /** @param argument topicName */
    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] split = argument.split(":");
      var topicName = split[0];
      final KafkaProducer<byte[], byte[]> kafkaProducer =
          org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();
      while (true) kafkaProducer.send(new ProducerRecord<>(topicName, new byte[10]));
    }
    @Override
    public String explainArgument() {
      return "(topic name)";
    }
  }

  public static class Consumer implements Workload {
    /**
     * @param bootstrapServer
     * @param argument topicName
     */
    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] split = argument.split(":");
      var topicName = split[0];
      final KafkaConsumer<?, ?> kafkaConsumer =
          new KafkaConsumer<>(
              Map.of(
                  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  bootstrapServer,
                  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                  "earliest"));

      while (true) {
        kafkaConsumer.poll(Duration.ofSeconds(1L));
      }
    }
    @Override
    public String explainArgument() {
      return "(topic name)";
    }
  }
  public static void main(String[] args) throws InterruptedException {
    Workload workloadProducer = new RealtimeApplication.Producer();
    workloadProducer.run("192.168.103.39:11300", "test-1:10");
    Workload workloadConsumer = new RealtimeApplication.Producer();
    workloadConsumer.run("192.168.103.39:11300", "test-1:10");
  }
}
