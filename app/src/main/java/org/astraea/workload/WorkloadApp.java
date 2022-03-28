package org.astraea.workload;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;

public class WorkloadApp {

  public static void main(String[] args) throws InterruptedException {
    if (args.length == 0) explain();
    else execute(args);
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
                  return new Thread(
                      () -> {
                        try {
                          classInstance.run(bootstrapServer, argument);
                        } catch (InterruptedException e) {
                          System.err.println("Thread has been interrupted");
                        }
                      });
                })
            .collect(Collectors.toList());

    threads.forEach(Thread::start);

    Utils.handleException(
        () -> {
          for (Thread thread : threads) {
            thread.join();
          }
          return true;
        });
  }

  static void explain() {
    // no DI so let's do thing the hard way.
    List<Class<?>> classes =
        List.of(
            RealtimeApplication.Producer.class,
            RealtimeApplication.Consumer.class,
            TimeRelatedApplication.Producer.class,
            TimeRelatedApplication.Consumer.class,
            OfflineLogProcessingApplication.Producer.class,
            OfflineLogProcessingApplication.Consumer.class,
            BurstLoading.Producer.class,
            BurstLoading.Consumer.class,
            HotKeyApplication.Producer.class,
            HotKeyApplication.Consumer.class);

    int maxClassNameSize =
        classes.stream().map(Class::getName).mapToInt(String::length).max().orElse(0);

    System.out.println(
        "Argument: (bootstrap.servers) [(workload 0 classpath) (workload 0 argument)]...");
    System.out.println();

    var format = "%-" + maxClassNameSize + "s   %s%n";
    System.out.printf(format, "[ClassName]", "[Argument Format]");
    for (Class<?> aClass : classes) {
      var instance = Utils.handleException(() -> (Workload) aClass.getConstructor().newInstance());
      System.out.printf(format, aClass.getName(), instance.explainArgument());
    }
  }
}
