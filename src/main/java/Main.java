import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {

  private static void createAndStartGrpServer(int port) throws Exception {
    ServerBuilder.forPort(port)
        .addService(new MyTestServiceGrpc.MyTestServiceImplBase() {
          @Override
          public void myTest(MyTestProto.Request request, StreamObserver<MyTestProto.Response> so) {
            if (request.getValue() == 0) {
              so.onNext(MyTestProto.Response.getDefaultInstance());
              so.onCompleted();
            }
          }
        })
        .build()
        .start();
  }

  private static EventLoopGroup createEventLoopGroup(String suffix) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("io-event-loop-" + suffix + "-%s").setDaemon(true)
            .build();

    if (Epoll.isAvailable()) {
      return new EpollEventLoopGroup(1, threadFactory);
    } else {
      return new KQueueEventLoopGroup(1, threadFactory);
    }
  }

  private static ManagedChannel buildChannel(String host, int port, boolean scheduledService,
      EventLoopGroup eventLoopGroup) {

    Class<? extends Channel> channelType;
    if (eventLoopGroup instanceof EpollEventLoopGroup) {
      channelType = EpollSocketChannel.class;
    } else if (eventLoopGroup instanceof KQueueEventLoopGroup) {
      channelType = KQueueSocketChannel.class;
    } else {
      throw new RuntimeException("unknown EventLoopGroup type");
    }

    System.out.println("Using " + channelType);

    NettyChannelBuilder builder = NettyChannelBuilder
        .forAddress(host, port)
        .eventLoopGroup(eventLoopGroup)
        .channelType(channelType)
        .defaultLoadBalancingPolicy("round_robin")
        // Doesn't matter, because we are using blocking stub
        //.executor(GrpcUtils.createExecutor("grpc-client-executor-", 50))
        .usePlaintext();
    if (scheduledService) {
      builder.scheduledExecutorService(Executors.newSingleThreadScheduledExecutor());
    }

    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    String host = "127.0.0.1";
    int port = 5658;

    createAndStartGrpServer(port);

    ScheduledExecutorService metricsScheduler = Executors.newSingleThreadScheduledExecutor();

    EventLoopGroup eventLoopGroupWithSchedulingLoops = createEventLoopGroup("scheduling");
    final MyTestServiceGrpc.MyTestServiceBlockingStub stub = MyTestServiceGrpc.newBlockingStub(
        buildChannel(host, port, false, eventLoopGroupWithSchedulingLoops));

    EventLoopGroup eventLoopGroupWithoutSchedulingLoops = createEventLoopGroup("no-scheduling");
    final MyTestServiceGrpc.MyTestServiceBlockingStub stubWithScheduledExecutor = MyTestServiceGrpc.newBlockingStub(
        buildChannel(host, port, true, eventLoopGroupWithoutSchedulingLoops));

    AtomicInteger pendingTasksWithSchedulingLoops = new AtomicInteger();
    AtomicInteger pendingTasksWithoutSchedulingLoops = new AtomicInteger();

    metricsScheduler.scheduleAtFixedRate(() -> countPendingTasks(List.of(
            new AbstractMap.SimpleEntry<>(pendingTasksWithSchedulingLoops, eventLoopGroupWithSchedulingLoops),
            new AbstractMap.SimpleEntry<>(pendingTasksWithoutSchedulingLoops, eventLoopGroupWithoutSchedulingLoops)))
        , 10, 10,
        TimeUnit.MILLISECONDS);


    // Warm-up calls
    MyTestProto.Request warmUpRequest = MyTestProto.Request.newBuilder().build();
    for (int i = 0; i < 5; i++) {
      stub.myTest(warmUpRequest);
      stubWithScheduledExecutor.myTest(warmUpRequest);
    }

    // The request asks the service to discard the request
    MyTestProto.Request request = MyTestProto.Request.newBuilder().setValue(100).build();
    int numTasks = 100;
    ExecutorService executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("main-grpc-request-%d").build());

    for (MyTestServiceGrpc.MyTestServiceBlockingStub currentStub : List.of(stub,
        stubWithScheduledExecutor, stub, stubWithScheduledExecutor, stub, stubWithScheduledExecutor,
        stub, stubWithScheduledExecutor)) {
      long totalTimePerExperiments = 0;
      for (int i = 0; i < 5; i++) {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numTasks);

        AtomicLong maxTime = new AtomicLong();
        AtomicLong minTime = new AtomicLong(Integer.MAX_VALUE);
        AtomicLong totalTime = new AtomicLong();

        for (int j = 0; j < numTasks; j++) {
          executorService.execute(() -> {
            try {
              try {
                startLatch.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              Stopwatch stopwatch = Stopwatch.createStarted();

              try {
                currentStub.withDeadlineAfter(50, TimeUnit.MILLISECONDS).myTest(request);
              } catch (StatusRuntimeException e) {
              }

              final long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
              maxTime.getAndAccumulate(time, Math::max);
              minTime.getAndAccumulate(time, Math::min);
              totalTime.addAndGet(time);
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              finishLatch.countDown();
            }
          });
        }

        startLatch.countDown();
        finishLatch.await();

        totalTimePerExperiments += totalTime.get();
        System.out.println(MessageFormat.format(
            "withScheduledService={0}, testNum={1}, maxTime={2}, minTime={3}, totalTime={4}",
            currentStub == stubWithScheduledExecutor, i, maxTime, minTime, totalTime));
        Thread.sleep(100);
      }
      System.out.println(
          MessageFormat.format("withScheduledService={0}, totalTimePerExperiments={1}",
              currentStub == stubWithScheduledExecutor, totalTimePerExperiments));
      Thread.sleep(1000);
    }

    System.out.println(
        MessageFormat.format("pending tasks withoutScheduledService={0}, withScheduledService={1}",
            pendingTasksWithSchedulingLoops.get(), pendingTasksWithoutSchedulingLoops.get()));

    System.exit(0);
  }

  private static void countPendingTasks(
      List<AbstractMap.Entry<AtomicInteger, EventLoopGroup>> pairs) {
    for (AbstractMap.Entry<AtomicInteger, EventLoopGroup> pair : pairs) {
      pair.getKey().getAndAccumulate(getPendingTasks(pair.getValue()), Math::max);
    }
  }

  private static int getPendingTasks(EventLoopGroup loopGroup) {
    int allPendingTasks = 0;
    for (EventExecutor eventExecutor : loopGroup) {
      if (eventExecutor instanceof SingleThreadEventExecutor executor) {
        allPendingTasks += executor.pendingTasks();
      }
    }
    return allPendingTasks;
  }
}
