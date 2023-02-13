/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.testBandWidth;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.consumer.Consumer;

public class TestBandWidth {
  public static void main(String[] args) {
    var bootstrapServer = "192.168.103.177:25655";
    var tpr = TopicPartitionReplica.of("test-1", 0, 0);

    try (var admin = Admin.of(bootstrapServer)) {
      // data folder migrate test
      admin
          .clusterInfo(Set.of(tpr.topic()))
          .toCompletableFuture()
          .get()
          .replicas()
          .forEach(
              r ->
                  System.out.println(
                      "broker: "
                          + tpr.brokerId()
                          + " tp: "
                          + tpr.topicPartition()
                          + "path: "
                          + r.path()
                          + " size: "
                          + r.size()));
      var startTime = (System.currentTimeMillis() / 1000.0);
       admin.moveToFolders(Map.of(tpr,"/tmp/log-folder-0")).toCompletableFuture().get();
      while (admin.clusterInfo(Set.of(tpr.topic())).toCompletableFuture().get().replicas().size()
          == 2)
        ;
      var endTime = (System.currentTimeMillis() / 1000.0);
      System.out.println(("execution time :" + (endTime - startTime)));
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    /*
    // consumer read test
    var consumer =
        Consumer.forPartitions(Set.of(tpr.topicPartition()))
            .bootstrapServers(bootstrapServer)
            .config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .build();

    var startTime = (System.currentTimeMillis() / 1000.0);
    while (true) {
      if (consumer.poll(Duration.ofSeconds(1)).isEmpty()) break;
    }
    var endTime = (System.currentTimeMillis() / 1000.0);
    System.out.println(("execution time :" + (endTime - startTime)));

     */
  }
}
