package parser;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.InlineScript;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.ScriptBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.ScriptQuery;
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
                .query("Денис")
        )._toQuery();

        Query byHeaderSevastopolMatch = MatchQuery.of(m -> m
                .field("header")
                .query("Севастополь")
        )._toQuery();

        Query byHeaderYaltaMatch = MatchQuery.of(m -> m
                .field("header")
                .query("Ялта")
        )._toQuery();

        Query byAuthorTermQuery = new Query.Builder().term( t -> t
                .field("author")
                .value(v -> v.stringValue("Денис Проничев"))
        ).build();

        Query byBodySevastopolMatch = MatchQuery.of(m -> m
                .field("body")
                .query("Севастополь")
        )._toQuery();

        // AND
        SearchResponse<NewsHeadline> andResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .must(byBodySevastopolMatch, byAuthorMatch)//, byHeaderSevastopolMatch)
                                )
                        ).size(10),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> andHits = andResponse.hits().hits();
        outputHits(andHits);

        // OR
        SearchResponse<NewsHeadline> orResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .should(byBodySevastopolMatch, byHeaderYaltaMatch)
                                )
                        ).size(10),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> orHits = orResponse.hits().hits();
        outputHits(orHits);

        // SCRIPT
//        InlineScript.Builder inlineScriptBuilder = new InlineScript.Builder();
//        InlineScript script = inlineScriptBuilder.source("doc['header'].value + ' ' + doc['author'].value").build();
//
//        SearchResponse scriptResponse = elcClient.search(s -> s
//                        .index(NEWS_HEADLINES_INDEX_NAME)
//                        .query(q -> q
//                                .script(sq -> sq.script(is -> is
//                                        .inline(script)
//                                ))
//                        ).size(10),
//                NewsHeadline.class
//        );
//
//        scriptResponse.hits().hits().forEach(hit -> {
//            // Process each hit document
//            System.out.println(hit.toString());
//        });


        // MULTIGET
//        SearchResponse<NewsHeadline> multiGetResponse = elcClient.search(s -> s
//                        .index(NEWS_HEADLINES_INDEX_NAME)
//                        .query(q -> q
//                                .multiMatch(b -> b.
//                                        .should(byBodySevastopolMatch, byHeaderYaltaMatch)
//                                )
//                        ).size(10),
//                NewsHeadline.class
//        );
//
//        List<Hit<NewsHeadline>> scriptHits = multiGetResponse.hits().hits();
//        outputHits(scriptHits);
    }

    private static void outputHits(List<Hit<NewsHeadline>> hits) {
        if (hits.isEmpty()) {
            logger.debug("Empty response");
        }
        for (Hit<NewsHeadline> hit: hits) {
            NewsHeadline newsHeadline = hit.source();
            assert newsHeadline != null;
            logger.debug("Found headline. Author: " + newsHeadline.GetAuthor() + " Headline: " + newsHeadline.GetHeader() + " URL: " + newsHeadline.GetURL() + "_id: " + hit.id() + " score: " + hit.score());
        }
        System.out.println();
    }
}