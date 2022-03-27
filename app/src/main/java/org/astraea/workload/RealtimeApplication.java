package org.astraea.workload;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.BiFunction;

public class RealtimeApplication {
    public static class Producer implements Workload {
    /**
     * @param bootstrapServer
     * @param argument        topicName,Producers:Consumers
     */

        @Override
        public void run(String bootstrapServer, String argument) {
           final String[] split = argument.split(":");
           final KafkaProducer<byte[], byte[]> kafkaProducer = org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();
           while (true)
                kafkaProducer.send(new ProducerRecord<>(split[0], new byte[100]));
        }

        public static void main(String[] args) throws InterruptedException {
            //  Workload workload = new RealtimeApplication();
          workload.run("192.168.103.39:11300", "test-1:1:10");
        }
    }
}


