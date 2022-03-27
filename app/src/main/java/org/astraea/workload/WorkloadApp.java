package org.astraea.workload;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;

public class WorkloadApp {

  public static void main(String[] args) throws InterruptedException {
    execute(args);
  }

  public static void execute(String... args) {
    System.out.println(String.join("", args));
    var bootstrapServer = args[0];
    var arguments = Arrays.stream(args).skip(1).collect(Collectors.toList());

    final var threads =
        IntStream.range(0, arguments.size() / 2)
            .mapToObj(
                index -> {
                  var classObject =
                      Utils.handleException(() -> Class.forName(arguments.get(index * 2)));
                  var classInstance =
                      Utils.handleException(
                          () -> (Workload) classObject.getConstructor().newInstance());
                  var argument = arguments.get(index * 2 + 1);
                  return new Thread(() -> classInstance.run(bootstrapServer, argument));
                })
            .collect(Collectors.toList());

    threads.forEach(Thread::start);
  }
}
