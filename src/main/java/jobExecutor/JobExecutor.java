package jobExecutor;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.Vector;

public class JobExecutor implements Executor {
    private final Queue<Runnable> jobQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean isRunning = true;
    private Vector<Thread> threads = null;
    private int threadsCounter = 0;

    public final boolean IsShutdown() {
        return !isRunning;
    }

    public final Vector<Thread> GetThreads() {
        return threads;
    }

    public final int GetThreadsCounter() {
        return threadsCounter;
    }

    private final class Job implements Runnable {

        @Override
        public void run() {
            while (isRunning) {
                Runnable nextJob = jobQueue.poll();
                if (nextJob != null) {
                    nextJob.run();
                }
            }
        }
    }

    public JobExecutor(int threadsCounterInitValue) {
        threads = new Vector<Thread>();
        for (int i = 0; i < threadsCounterInitValue; i++) {
            threads.add(new Thread(new Job()));
            threads.get(i).start();
        }

        threadsCounter = threadsCounterInitValue;
    }

    @Override
    public void execute(Runnable command) {
        if (isRunning) {
            jobQueue.offer(command);
        }
    }

    public void executeTasks(Queue<Runnable> tasks) {
        for (int i = 0; i < tasks.size(); i++) {
            execute(tasks.poll());
        }
    }

    public void shutdown() {
        isRunning = false;
    }

    public void joinThread(int threadIndex) throws InterruptedException {
        threads.get(threadIndex).join();
    }

    public void joinAllThreads() throws InterruptedException {
        for (int i=0; i<threadsCounter; ++i) {
            joinThread(i);
        }
    }
}
