package paris.benoit.mob.front;

import org.eclipse.jetty.util.thread.ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JettyLoomThreadPool implements ThreadPool {
    // TODO bump quand asm sera OK avec du Java 16+ (on a du EA Loom sur 16)
//    ExecutorService executorService = Executors.newVirtualThreadExecutor();
    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    @Override
    public void join() throws InterruptedException {
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    @Override
    public int getThreads() {
        return 1;
    }

    @Override
    public int getIdleThreads() {
        return 1;
    }

    @Override
    public boolean isLowOnThreads() {
        return false;
    }

    @Override
    public void execute(Runnable command) {
        executorService.submit(command);
    }

}