package parser;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetOperation;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;

import entities.NewsHeadline;
import jobExecutor.JobExecutor;
import org.elasticsearch.search.SearchHit;
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

//        ExecuteSearchRequests();
    }

    public static void ExecuteSearchRequests() throws IOException {
        WebsiteParser.ElasticClient ec = new WebsiteParser.ElasticClient(SERVER_URL, API_KEY);
        elcClient = ec.elasticRestClient();

        Query byAuthorMatch = MatchQuery.of(m -> m
                .field("author")
                .query("Денис Проничев")
        )._toQuery();

        Query byBodySimferopolMatch = MatchQuery.of(m -> m
                .field("body")
                .query("Симферополь")
        )._toQuery();

        Query byHeaderYaltaMatch = MatchQuery.of(m -> m
                .field("header")
                .query("Ялта")
        )._toQuery();

        Query byAuthorTermQuery = new Query.Builder().term( t -> t
                .field("author")
                .value(v -> v.stringValue("Денис Проничев"))
        ).build();

        // AND
        SearchResponse<NewsHeadline> andResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .must(byBodySimferopolMatch, byAuthorMatch)//, byHeaderSevastopolMatch)
                                )
                        ),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> andHits = andResponse.hits().hits();
        outputHits(andHits);

        // OR
        SearchResponse<NewsHeadline> orResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .should(byBodySimferopolMatch, byAuthorMatch)
                                )
                        ),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> orHits = orResponse.hits().hits();
        outputHits(orHits);

        // SCRIPT
        SearchResponse<NewsHeadline> scriptResponse = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .query(q -> q
                                .scriptScore(ss -> ss
                                        .query(q1 -> q1
                                                .matchAll(ma -> ma))
                                        .script(scr -> scr
                                                .inline(i -> i
                                                        .source("doc['URL'].value.length()"))))),
                NewsHeadline.class
        );

        List<Hit<NewsHeadline>> scriptHits = scriptResponse.hits().hits();
        outputHits(scriptHits);


        // MULTIGET
        MgetResponse<NewsHeadline> mgetResponse = elcClient.mget(mgq -> mgq
                .index(NEWS_HEADLINES_INDEX_NAME)
                        .docs(d -> d
                                .id("arr7Vo8B5aM0OFyPWyIm")
                                .id("V1q0WY8Bq03dI9uQAZuB")
                                .id("X7r7Vo8B5aM0OFyPSSL_")),

                NewsHeadline.class
        );
        List<NewsHeadline> mgetHits = new ArrayList<>();
        mgetHits.add(mgetResponse.docs().getFirst().result().source());
        for (NewsHeadline newsHeadline: mgetHits) {
            assert newsHeadline != null;
            logger.debug("Found headline. Author: " + newsHeadline.GetAuthor() + " Headline: " + newsHeadline.GetHeader() + " URL: " + newsHeadline.GetURL());
        }
        System.out.println();



        // Date Histogram Aggregation
        Aggregation agg1 = Aggregation.of(a -> a.dateHistogram(dha -> dha.field("date").calendarInterval(CalendarInterval.valueOf(String.valueOf(CalendarInterval.Day)))));
        SearchResponse<?> dhAggregation = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .aggregations("articles_per_day", agg1),
                NewsHeadline.class
        );

        logger.debug(String.valueOf(dhAggregation));
        System.out.println();

        // Date Range Aggregation
        Aggregation agg2 = Aggregation.of(a -> a.dateRange(dha -> dha.field("date")
                .ranges(dr -> dr
                        .from(FieldDateMath.of(fdm -> fdm.expr("2024-01-01")))
                        .to(FieldDateMath.of(fdm -> fdm.expr("2024-02-01"))))));
        SearchResponse<?> drAggregation = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .aggregations("articles_in_range", agg2),
                NewsHeadline.class
        );

        logger.debug(String.valueOf(drAggregation));
        System.out.println();

        // Histogram Aggregation
        Aggregation agg3 = Aggregation.of(a -> a.histogram(dha -> dha.script(scr -> scr
                        .inline(i -> i
                                .source("doc['header'].value.length()")
                                .lang("painless"))
                ).interval(10.0)
                ));
        SearchResponse<?> hAggregation = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .aggregations("header_length_histogram", agg3),
                NewsHeadline.class
        );

        logger.debug(String.valueOf(hAggregation));
        System.out.println();

        // Terms Aggregation
        Aggregation agg4 = Aggregation.of(a -> a.terms(t -> t
                .field("author")
                )
        );
        SearchResponse<?> tAggregation = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .aggregations("popular_authors", agg4),
                NewsHeadline.class
        );

        logger.debug(String.valueOf(tAggregation));
        System.out.println();

        // Filter Aggregation
        Aggregation agg5_1 = Aggregation.of(a -> a
                .avg(avg -> avg
                        .script(scr -> scr
                                .inline(i -> i
                                        .source("doc['body'].value.length()")
                                        .lang("painless"))
                        )
                )
        );
        Aggregation agg5 = Aggregation.of(a -> a
                .filter(q -> q.term(t -> t
                                .field("author")
                                .value("crimea.mk.ru")
                        )
                )
                .aggregations("avg_body_length", agg5_1)
        );
        SearchResponse<?> fAggregation = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .aggregations("filtered_body", agg5),
                NewsHeadline.class
        );

        logger.debug(String.valueOf(fAggregation));
        System.out.println();

        // Logs Aggregation
        Aggregation agg6 = Aggregation.of(a -> a.terms(t -> t
                        .field("stream.keyword")
                        .size(10)
                )
        );
        SearchResponse<?> lAggregation = elcClient.search(s -> s
                        .index(NEWS_HEADLINES_INDEX_NAME)
                        .aggregations("streams", agg6)
                        .size(0),
                NewsHeadline.class
        );

        logger.debug(String.valueOf(lAggregation));
        System.out.println();
    }

    private static void outputHits(List<Hit<NewsHeadline>> hits) {
        if (hits.isEmpty()) {
            logger.debug("Empty response");
        }
        for (Hit<NewsHeadline> hit: hits) {
            NewsHeadline newsHeadline = hit.source();
            assert newsHeadline != null;
            logger.debug("Found headline. Author: " + newsHeadline.GetAuthor() + " Headline: " + newsHeadline.GetHeader() + " URL: " + newsHeadline.GetURL() + " _id: " + hit.id() + " score: " + hit.score());
        }
        System.out.println();
    }
}