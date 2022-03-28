package org.astraea.workload;

public interface Workload {

  /**
   * The workload logic. Each workload will run in a standalone thread. The workload logic must
   * handle thread interrupt correctly. Once an interrupt happened, the workload should stop and
   * leave as soon as possible.
   *
   * @param bootstrapServer the kafka server addresses
   * @param argument a string argument for this workflow
   */
  void run(String bootstrapServer, String argument) throws InterruptedException;

  /**
   * Explain the workload argument format
   * @return a string describe the format of the workflow argument
   */
  default String explainArgument() {
    return "The author refuse to explain :3";
  }
}
