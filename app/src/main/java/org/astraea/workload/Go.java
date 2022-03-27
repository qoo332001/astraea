package org.astraea.workload;

public class Go {
  public static void main(String[] args) {
    WorkloadApp.execute(
        "192.168.103.177:10001,192.168.103.177:10002,192.168.103.177:10003",
        TimeRelatedApplication.Producer.class.getName(),
        "10000,topicA:topicB:topicC:topicD:topicE",
        TimeRelatedApplication.Consumer.class.getName(),
        "group,topicA:topicB:topicC:topicD:topicE",
        TimeRelatedApplication.Consumer.class.getName(),
        "group,topicA:topicB:topicC:topicD:topicE",
        OfflineLogProcessingApplication.Producer.class.getName(),
        "1000:offline-log-processing-01",
        OfflineLogProcessingApplication.Consumer.class.getName(),
        "proc-group:offline-log-processing-01");
  }
}
