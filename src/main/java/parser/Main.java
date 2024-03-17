package parser;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import jobExecutor.JobExecutor;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException  {
        System.out.println("Setting up environment");
        JobExecutor threadPool = new JobExecutor(2);
        Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();

        Runnable runRMQ = () -> {
            try {
                RabbitProducer.runProducer();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        tasks.add(runRMQ);
        tasks.add(runRMQ);

        threadPool.executeTasks(tasks);

        threadPool.shutdown();

        threadPool.joinAllThreads();
    }
}