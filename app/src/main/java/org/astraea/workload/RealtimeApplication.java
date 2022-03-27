package org.astraea.workload;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Time;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Set;
import java.util.function.BiFunction;

public class RealtimeApplication {
    public static class Producer implements Workload {
    /**
     * @param bootstrapServer
     * @param argument        topicName:Producers
     */

        @Override
        public void run(String bootstrapServer, String argument) {
           final String[] split = argument.split(":");
           var topicName=split[0];
           final KafkaProducer<byte[], byte[]> kafkaProducer = org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();
           while (true)
                kafkaProducer.send(new ProducerRecord<>(topicName, new byte[10]));
        }
    }
    public static class Consumer implements Workload {
        /**
         * @param bootstrapServer
         * @param argument        topicName:Consumers
         */
        @Override
        public void run(String bootstrapServer, String argument) {
            final String[] split = argument.split(":");
            var topicName=split[0];
            final org.astraea.consumer.Consumer<byte[], byte[]> build =
                    org.astraea.consumer.Consumer.builder()
                            .brokers(bootstrapServer)
                            .topics(Set.of(topicName))
                            .build();
            while (true)
                build.poll(Duration.ofSeconds(1L));
        }
    }
    public static void main(String[] args) throws InterruptedException {
        Workload workloadProducer = new RealtimeApplication.Consumer();
        workloadProducer.run("192.168.103.39:11300", "test-1:1:10");

    }
}


