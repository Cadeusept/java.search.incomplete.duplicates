package parser;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import jobExecutor.JobExecutor;

public class Main {
    public static final String BasePath = "https://crimea.mk.ru";
    public static final int Depth = 1;
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException, TimeoutException {
        System.out.println("Setting up environment");
        JobExecutor threadPool = new JobExecutor(5);
        Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();
        WebsiteParser parserEntity = new WebsiteParser();

        Runnable runLinkCatcher = () -> {
            try {
                parserEntity.runLinkCatcher(Depth, BasePath);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        tasks.add(runLinkCatcher);
        //tasks.add(runLinkCatcher);

        Runnable runHtmlParser = () -> {
            try {
                parserEntity.RunHtmlParserAndElcProducer();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        tasks.add(runHtmlParser);
        tasks.add(runHtmlParser);
        tasks.add(runHtmlParser);

        Runnable runElcConsumer = () -> {
            try {
                parserEntity.RunElcConsumer();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        tasks.add(runElcConsumer);

        threadPool.executeTasks(tasks);

        threadPool.shutdown();

        threadPool.joinAllThreads();
    }
}