package parser;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import entities.NewsHeadline;
import jobExecutor.JobExecutor;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    public static final String BasePath = "https://crimea.mk.ru";
    public static final int Depth = 1;
    private static final String SERVER_URL = "http://localhost:9200";
    private static final String API_KEY = "";
    private static final String NEWS_HEADLINES_INDEX_NAME = "news_headlines";
    public static ElasticsearchClient elcClient;
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException, TimeoutException {
        logger.debug("Setting up environment");
        JobExecutor threadPool = new JobExecutor(7);
        Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();
        WebsiteParser parserEntity = new WebsiteParser();

        Runnable runLinkCatcher = () -> {
            try {
                parserEntity.runLinkCatcher(Depth, BasePath);
            } catch (IOException | TimeoutException e) {
                logger.error(String.valueOf(e));
            } catch (NoSuchAlgorithmException e) {
                logger.error(String.valueOf(e));
                throw new RuntimeException(e);
            }
        };

        tasks.add(runLinkCatcher);

        Runnable runHtmlParser = () -> {
            try {
                parserEntity.RunHtmlParserAndElcProducer();
            } catch (IOException | TimeoutException e) {
                logger.error(String.valueOf(e));
            } catch (InterruptedException e) {
                logger.error(String.valueOf(e));
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
                logger.error(String.valueOf(e));
            } catch (InterruptedException e) {
                logger.error(String.valueOf(e));
                throw new RuntimeException(e);
            }
        };

        tasks.add(runElcConsumer);
        tasks.add(runElcConsumer);
        tasks.add(runElcConsumer);

        threadPool.executeTasks(tasks);

        threadPool.shutdown();

        threadPool.joinAllThreads();

        ExecuteSearchRequests();
    }

    public static void ExecuteSearchRequests() throws IOException {
        WebsiteParser.ElasticClient ec = new WebsiteParser.ElasticClient(SERVER_URL, API_KEY);
        elcClient = ec.elasticRestClient();

        Query byAuthorMatch = MatchQuery.of(m -> m
                .field("author")
                .query("Денис Проничев")
        )._toQuery();

        Query byHeaderSevastopolMatch = MatchQuery.of(m -> m
                .field("header")
                .query("Севас")
        )._toQuery();

        Query byHeaderYaltaMatch = MatchQuery.of(m -> m
                .field("header")
                .query("Ялт")
        )._toQuery();

        Query byAuthorTermQuery = new Query.Builder().term( t -> t
                .field("author")
                .value(v -> v.stringValue("Денис Проничев"))
        ).build();

        Query byHeaderSevastopolTerm = new Query.Builder().term( t -> t
                .field("body")
                .value(v -> v.stringValue("Севастополь"))
        ).build();

        Query byHeaderYaltaTerm = new Query.Builder().term( t -> t
                .field("header")
                .value(v -> v.stringValue("Ялт"))
        ).build();


        SearchResponse<NewsHeadline> andResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .must(byAuthorTermQuery)
                                        .must(byHeaderSevastopolTerm)
                                )
                        ).size(10),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> andHits = andResponse.hits().hits();
        outputHits(andHits);

        SearchResponse<NewsHeadline> orResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .must(byAuthorMatch)
                                        .should(byHeaderSevastopolMatch)
                                        .should(byHeaderYaltaMatch)
                                )
                        ).size(10),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> orHits = orResponse.hits().hits();
        outputHits(orHits);
    }

    private static void outputHits(List<Hit<NewsHeadline>> hits) {
        if (hits.isEmpty()) {
            logger.debug("Empty response");
            return;
        }
        for (Hit<NewsHeadline> hit: hits) {
            NewsHeadline newsHeadline = hit.source();
            assert newsHeadline != null;
            logger.debug("Found headline " + newsHeadline.GetAuthor() + " " + newsHeadline.GetHeader() + " " + newsHeadline.GetURL() + ", score " + hit.score());
        }
    }
}