package org.astraea.workload;

public interface Workload {

  /** Run the shitty workload */
  void run(String bootstrapServer, String argument);
}
