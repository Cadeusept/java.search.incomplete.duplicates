package parser;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import jobExecutor.JobExecutor;

public class Main {
    public static final String BasePath = "https://crimea.mk.ru";
    public static void main(String[] args) throws ExecutionException, InterruptedException  {
        System.out.println("Setting up environment");
        JobExecutor threadPool = new JobExecutor(2);
        Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();

        Runnable runLinkCatcher = () -> {
            try {
                WebsiteParser.runLinkCatcher(1, BasePath);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        //tasks.add(runLinkCatcher);
        tasks.add(runLinkCatcher);

        threadPool.executeTasks(tasks);

        Runnable runHtmlParser = () -> {
            try {
                WebsiteParser.runParser(1, BasePath);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        //tasks.add(runRMQ);
        tasks.add(runHtmlParser);

        threadPool.executeTasks(tasks);

        threadPool.shutdown();

        threadPool.joinAllThreads();
    }
}