package parser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.http.client.CookieStore;

import jobExecutor.JobExecutor;
import org.jsoup.nodes.Document;

public class Main {
    public static final String BasePath = "https://crimea.mk.ru";
    public static final int Depth = 1;
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException, TimeoutException {
        System.out.println("Setting up environment");
        JobExecutor threadPool = new JobExecutor(2);
        Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();
        WebsiteParser parserEntity = new WebsiteParser();

        Runnable runLinkCatcher = () -> {
            try {
                parserEntity.runLinkCatcher(Depth, BasePath);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        //tasks.add(runLinkCatcher);
        //tasks.add(runLinkCatcher);

        threadPool.executeTasks(tasks);

        HashMap<String, Document> docVec = new HashMap<String, Document>();

        Runnable runHtmlParser = () -> {
            try {
                parserEntity.RunHtmlParser(docVec);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        //tasks.add(runRMQ);
        tasks.add(runHtmlParser);

        threadPool.executeTasks(tasks);

        Runnable runElkProducer = () -> {
            try {
                parserEntity.RunElkProducer(docVec);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        tasks.add(runElkProducer);

        threadPool.shutdown();

        threadPool.joinAllThreads();
    }
}