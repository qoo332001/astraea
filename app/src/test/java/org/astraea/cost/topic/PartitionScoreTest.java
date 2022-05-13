package org.astraea.cost.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.astraea.admin.Admin;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PartitionScoreTest extends RequireBrokerCluster {
  static Admin admin;

  @BeforeAll
  static void setup() throws ExecutionException, InterruptedException {
    admin = Admin.of(bootstrapServers());
    Map<Integer, String> topicName = new HashMap<>();
    topicName.put(0, "testPartitionScore0");
    topicName.put(1, "testPartitionScore1");
    topicName.put(2, "__consumer_offsets");
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName.get(0))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(1))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(2))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .keySerializer(Serializer.STRING)
            .build();
    int size = 10000;
    for (int t = 0; t <= 1; t++) {
      for (int p = 0; p <= 3; p++) {
        producer
            .sender()
            .topic(topicName.get(t))
            .partition(p)
            .value(new byte[size])
            .run()
            .toCompletableFuture()
            .get();
      }
      size += 10000;
    }
    producer.close();
  }

  @Test
  void testGetScore() {
    PartitionScore.Argument argument = new PartitionScore.Argument();
    argument.excludeInternalTopic = false;
    var score = PartitionScore.execute(argument, admin);
    assertEquals(3, score.size());
    assertEquals(3 * 4, score.get(0).size() + score.get(1).size() + score.get(2).size());
    argument.excludeInternalTopic = true;
    score = PartitionScore.execute(argument, admin);
    assertEquals(3, score.size());
    assertEquals(2 * 4, score.get(0).size() + score.get(1).size() + score.get(2).size());
  }
}
