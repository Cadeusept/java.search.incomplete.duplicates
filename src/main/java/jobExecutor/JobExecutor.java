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
            while (isRunning || !jobQueue.isEmpty()) {
                Runnable nextJob = jobQueue.poll();
                if (nextJob != null) {
                    nextJob.run();
                } else {
                    // If no job is available, wait for a short time before checking again
                    try {
                        Thread.sleep(10); // You can adjust the sleep duration as needed
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
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
        while (!tasks.isEmpty()) {
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
