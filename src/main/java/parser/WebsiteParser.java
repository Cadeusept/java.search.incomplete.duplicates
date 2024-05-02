package parser;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import entities.Log;
import entities.NewsHeadline;
import java.io.IOException;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import entities.Link;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpEntity;

public class WebsiteParser extends Thread {
    private static final String LOGGER_LEVEL_INFO = "info";
    private static final String LOGGER_LEVEL_WARNING = "warning";
    private static final String LOGGER_LEVEL_ERROR = "error";
    private static final String DATA_QUEUE_NAME = "data_queue";
    private static final String URL_QUEUE_NAME = "url_queue";
    private static final String RMQ_HOST_NAME = "localhost";
    private static final int RMQ_PORT = 5673;
    private static final String RMQ_USERNAME = "rmq_dev";
    private static final String RMQ_PASSWORD = "password";
    private static final String SERVER_URL = "http://localhost:9200";
    private static final String API_KEY = "";
    private static final String LOGS_INDEX_NAME = "logs";
    private static final String NEWS_HEADLINES_INDEX_NAME = "news_headlines";
    private static final Object indexCreationLock = new Object();
    private final logIndexer logger = new logIndexer();

    public WebsiteParser() throws IOException {}

    private static class logIndexer {
        private final ObjectMapper mapper = new ObjectMapper();
        private ElasticsearchClient elcClient;
        public logIndexer() throws IOException {
            logIndexer.ElasticLoggingClient ec = new ElasticLoggingClient(SERVER_URL, API_KEY);
            elcClient = ec.elasticRestClient();
            mapper.registerModule(new JodaModule());
        }

        public class ElasticLoggingClient {

            private String serverUrl;
            private String apiKey;

            public ElasticLoggingClient(String serverUrl, String apiKey) throws IOException {
                this.serverUrl=serverUrl;
                this.apiKey=apiKey;
            }

            public ElasticsearchClient elasticRestClient() throws IOException {

                // Create the low-level client
                RestClient restClient = RestClient
                        .builder(HttpHost.create(serverUrl))
                        .setDefaultHeaders(new Header[]{
                                new BasicHeader("Authorization", "ApiKey " + apiKey)
                        })
                        .build();

                // The transport layer of the Elasticsearch client requires a json object mapper to
                // define how to serialize/deserialize java objects. The mapper can be customized by adding
                // modules, for example since the Article and Comment object both have Instant fields, the
                // JavaTimeModule is added to provide support for java 8 Time classes, which the mapper itself does
                // not support.
                ObjectMapper mapper = JsonMapper.builder()
                        .addModule(new JavaTimeModule())
                        .build();

                // Create the transport with the Jackson mapper
                ElasticsearchTransport transport = new RestClientTransport(
                        restClient, new JacksonJsonpMapper(mapper));

                // Create the API client
                ElasticsearchClient esClient = new ElasticsearchClient(transport);

                // Creating the indexes
                createIndexWithDateMappingLogs(esClient, LOGS_INDEX_NAME);

                return esClient;
            }

            private void createIndexWithDateMappingLogs(ElasticsearchClient esClient, String index) throws IOException {
                synchronized (indexCreationLock) {
                    BooleanResponse indexRes = esClient.indices().exists(ex -> ex.index(index));
                    if (!indexRes.value()) {
                        esClient.indices().create(c -> c
                                .index(index)
                                .mappings(m -> m
                                        .properties("created_at", p -> p
                                                .date(d -> d.format("strict_date_optional_time")))
                                        .properties("level", p -> p.keyword(d -> d))
                                        .properties("message", p -> p.keyword(d -> d))
                                ));

                    }
                }
            }
        }

        public void indexLog(String level, String msg) throws IOException {
            try {
                Log lm = new Log(level, msg);

                System.out.println(lm.GetCreatedAt() + " " + lm.GetLevel() + " " + lm.GetMessage());

                IndexRequest<Log> indexReq = IndexRequest.of((id -> id
                        .index(LOGS_INDEX_NAME)
                        .refresh(Refresh.WaitFor)
                        .document(lm)));

                IndexResponse indexResponse = elcClient.index(indexReq);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static class linkCatcher {
        private String baseUrl;
        private int depth_count = 1;
        public Deque<Link> urlVec = null;
        public Deque<Link> resultUrlVec = null;
        private Channel rmqChan = null;
        private logIndexer logger = null;

        public linkCatcher(int depth, String inputBaseUrl, Channel rmqChannel, logIndexer l) throws IOException, TimeoutException {
            urlVec = new ArrayDeque<Link>();
            resultUrlVec = new ArrayDeque<Link>();
            baseUrl = inputBaseUrl;
            urlVec.add(new Link(baseUrl, 0));
            depth_count = depth;
            rmqChan = rmqChannel;
            logger = l;
        }

        public void Start() throws IOException {
            for (int i = 0; i < depth_count; ++i) {
                fork();
                logger.indexLog(LOGGER_LEVEL_INFO, Thread.currentThread() + "start work");
                urlVec.addAll(resultUrlVec);
            }

            logger.indexLog(LOGGER_LEVEL_INFO, Thread.currentThread() + "end work, " + urlVec.size() + "links");
        }

        private void fork() throws IOException {
            Link cur;

            while ((cur = urlVec.pollFirst()) != null) {
                parseUrlAndPublishPage(cur);
            }
        }

        private void parseUrlAndPublishPage(Link url) throws IOException {
            int level = url.GetLevel() + 1;

            Document doc = Jsoup.connect(url.GetUrl()).get();
            Elements links = doc.select("a[href]");

            for (Element link : links) {
                if (
                        !link.attr("abs:href").startsWith(baseUrl + "/politics/2024/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/incident/2024/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/culture/2024/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/social/2024/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/economics/2024/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/science/2024/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/sport/2024/")
                ) {
                    continue;
                }

                rmqChan.basicPublish("", URL_QUEUE_NAME, null, link.attr("abs:href").getBytes(StandardCharsets.UTF_8));

                resultUrlVec.add(new Link(link.attr("abs:href"), level));
                if (level <= this.depth_count) {
                    urlVec.add(new Link(link.attr("abs:href"), level));
                }
            }
        }
    }

    public void runLinkCatcher(int depth, String inputBaseUrl) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DATA_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);

        linkCatcher prod = new linkCatcher(depth, inputBaseUrl, channel, logger);
        prod.Start();

        channel.close();
        connection.close();
    }

    private static class htmlParser {
        private CloseableHttpClient client = null;
        private Channel rmqChan = null;
        private final int retryCount = 3;
        private final int metadataTimeout = 30 * 1000;
        private final int retryDelay = 5 * 1000;
        private static volatile  Map<String, Document> docVec;
        private logIndexer logger = null;

        public htmlParser(Map<String, Document> docVec, Channel rmqChannel, logIndexer logger) throws IOException, TimeoutException {
            rmqChan = rmqChannel;
            this.docVec = docVec;
            this.logger = logger;

            client = HttpClients.custom()
                    .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                    .setDefaultCookieStore(new BasicCookieStore()).build();
        }

        public void parsePage(String url) throws IOException {
            int code = 0;
            boolean bStop = false;
            Document doc = null;
            for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
                //  log.info("getting page from url " + url);
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(metadataTimeout)
                        .setConnectTimeout(metadataTimeout)
                        .setConnectionRequestTimeout(metadataTimeout)
                        .setExpectContinueEnabled(true)
                        .build();
                HttpGet request = new HttpGet(url);
                request.setConfig(requestConfig);
                CloseableHttpResponse response = null;
                try {
                    logger.indexLog(LOGGER_LEVEL_INFO, Thread.currentThread() + "start");
                    response = client.execute(request);
                    logger.indexLog(LOGGER_LEVEL_INFO, Thread.currentThread() + "stop");
                    code = response.getStatusLine().getStatusCode();
                    if (code == 404) {
                        logger.indexLog(LOGGER_LEVEL_ERROR, "error get url " + url + " code " + code);
                        try {
                            response.close();
                        } catch (IOException e) {
                            logger.indexLog(LOGGER_LEVEL_ERROR, String.valueOf(e));
                        }
                        logger.indexLog(LOGGER_LEVEL_WARNING, "error get url " + url + " code " + code);
                        bStop = true;
                    } else if (code == 200) {
                        HttpEntity entity = response.getEntity();
                        if (entity != null) {
                            try {
                                doc = Jsoup.parse(entity.getContent(), "UTF-8", url);
                                docVec.put(url, doc);
                                //logger.indexLog(LOGGER_LEVEL_INFO, docVec.size() + "docs downloaded");
                                try {
                                    response.close();
                                } catch (IOException e) {
                                    logger.indexLog(LOGGER_LEVEL_ERROR, String.valueOf(e));
                                }
                                break;
                            } catch (IOException e) {
                                logger.indexLog(LOGGER_LEVEL_INFO, String.valueOf(e));
                            }
                        }
                        bStop = true;
                    } else {
                        logger.indexLog(LOGGER_LEVEL_WARNING, "error get url " + url + " code " + code);
                        // log.warn("error get url " + url + " code " + code);
                        response.close();
                        response = null;
                        client.close();
                        // CookieStore httpCookieStore = new BasicCookieStore();
                        client = HttpClients.custom()
                                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                                .setDefaultCookieStore(new BasicCookieStore()).build();
                        int delay = retryDelay * 1000 * (iTry + 1);
                        // log.info("wait " + delay / 1000 + " s...");
                        try {
                            Thread.sleep(delay);
                            continue;
                        } catch (InterruptedException ex) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    logger.indexLog(LOGGER_LEVEL_ERROR, String.valueOf(e));
                }
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException e) {
                        logger.indexLog(LOGGER_LEVEL_ERROR, String.valueOf(e));
                    }
                }
            }
        }
    }

    public void RunHtmlParserAndElcProducer() throws IOException, TimeoutException, InterruptedException {
        Map<String, Document> docVec = Collections.synchronizedMap(new ConcurrentHashMap<>());

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DATA_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);

        htmlParser cons = new htmlParser(docVec, channel, logger);

        channel.basicConsume(URL_QUEUE_NAME, false, "javaConsumerTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                String message = new String(body, StandardCharsets.UTF_8);
                logger.indexLog(LOGGER_LEVEL_INFO, " Received '" + message + "'" + "  " + Thread.currentThread());
                cons.parsePage(message);
                channel.basicAck(deliveryTag, false);
            }
        });

        int responceWaitCount = 0;

        final int retryCount = 20;

        while (responceWaitCount<retryCount) {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(URL_QUEUE_NAME);
            if (response.getMessageCount() != 0) {
                responceWaitCount = 0;
                Thread.sleep(500);
            } else {
                Thread.sleep(5000);
                responceWaitCount++;
                logger.indexLog(LOGGER_LEVEL_INFO, "Waiting for messages in "+ URL_QUEUE_NAME +", " + (retryCount-responceWaitCount) * 5 + " seconds until shutdown" + Thread.currentThread());
            }
        }

        channel.basicCancel("javaConsumerTag");
        channel.close();
        connection.close();

        RunElkProducer(docVec);
    }

    private static class elkProducer {
        private Channel rmqChan = null;
        private final ObjectMapper mapper = new ObjectMapper();
        private logIndexer logger = null;

        public elkProducer(Channel channel, logIndexer logger) {
            rmqChan = channel;
            this.logger = logger;
        }

        public void ParsePublishNews(Map<String, Document> docVec) throws InterruptedException, IOException {
            if (docVec.isEmpty()) {
                logger.indexLog(LOGGER_LEVEL_INFO, "empty map");
            } else {
                for (Map.Entry<String, Document> entry : docVec.entrySet()) {
                    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
                    // parsePrintNews(entry.getKey(), entry.getValue());   // dev
                    parseProduceToElk(entry.getKey(), entry.getValue()); // TODO prod
                }
                Thread.sleep(500);
            }
        }

        private void parsePrintNews(String url, Document doc) throws IOException {
            try {
                logger.indexLog(LOGGER_LEVEL_INFO, "Header:");
                logger.indexLog(LOGGER_LEVEL_INFO, doc.select("div [class=article__title]").getFirst().text());
                logger.indexLog(LOGGER_LEVEL_INFO, "Body:");
                logger.indexLog(LOGGER_LEVEL_INFO, doc.select("div [class=article__body]").getFirst().text());
                logger.indexLog(LOGGER_LEVEL_INFO, "Author:");
                logger.indexLog(LOGGER_LEVEL_INFO, doc.select("li [class=article__author-text-link]").getFirst().text());
                logger.indexLog(LOGGER_LEVEL_INFO, "Date:");
                logger.indexLog(LOGGER_LEVEL_INFO, doc.select("time").getFirst().attr("datetime"));
                logger.indexLog(LOGGER_LEVEL_INFO, "URL:");
                logger.indexLog(LOGGER_LEVEL_INFO, url);
            } catch (Exception e) {
                logger.indexLog(LOGGER_LEVEL_ERROR, String.valueOf(e));
            }
        }

        public void parseProduceToElk(String url, Document doc) throws IOException {
            NewsHeadline newsHeadline = new NewsHeadline();
            try {
                newsHeadline.SetHeader(doc.select("div [class=article__title]").getFirst().text());
                newsHeadline.SetBody(doc.select("div [class=article__body]").getFirst().text());
                newsHeadline.SetAuthor(doc.select("li [class=article__author-text-link]").getFirst().text());
                DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
                String s = doc.select("time").getFirst().attr("datetime");

                // Extracting the timezone offset string
                String timeZoneOffset = s.substring(s.length() - 5);
                // Removing the colon from the timezone offset
                String formattedDateTimeString = s.substring(0, s.length() - 5) + timeZoneOffset;
                DateTime dateTime = f.parseDateTime(formattedDateTimeString);
                // Create a formatter for ISO 8601 date format
                DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
                // Format the DateTime object as an ISO 8601 string
                String iso8601String = formatter.print(dateTime);
                newsHeadline.SetDate(iso8601String);

                newsHeadline.SetURL(url);
                rmqChan.basicPublish("", DATA_QUEUE_NAME, null, mapper.writeValueAsBytes(newsHeadline));
            } catch (Exception e) {
                logger.indexLog(LOGGER_LEVEL_ERROR, String.valueOf(e));
            }
        }
    }
    public void RunElkProducer(Map<String, Document> docVec) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DATA_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);


        elkProducer prod = new elkProducer(channel, logger);

        prod.ParsePublishNews(docVec);

        channel.close();
        connection.close();
    }

    private static class elcConsumer {
        private Channel rmqChan = null;
        private String serverUrl;
        private String apiKey;
        private final ObjectMapper mapper = new ObjectMapper();
        private final String LOGS_INDEX_NAME = "logs";
        private ElasticsearchClient elcClient;
        private logIndexer logger = null;

        public elcConsumer(Map<String, Document> docVec, Channel rmqChannel, logIndexer logger) throws IOException {
            rmqChan = rmqChannel;
            ElasticClient ec = new ElasticClient(SERVER_URL, API_KEY);
            elcClient = ec.elasticRestClient();
            mapper.registerModule(new JodaModule());
            this.logger = logger;
        }

        public class ElasticClient {

            private String serverUrl;
            private String apiKey;
            
            public ElasticClient(String serverUrl, String apiKey) throws IOException {
                this.serverUrl=serverUrl;
                this.apiKey=apiKey;
            }

            public ElasticsearchClient elasticRestClient() throws IOException {

                // Create the low-level client
                RestClient restClient = RestClient
                        .builder(HttpHost.create(serverUrl))
                        .setDefaultHeaders(new Header[]{
                                new BasicHeader("Authorization", "ApiKey " + apiKey)
                        })
                        .build();

                // The transport layer of the Elasticsearch client requires a json object mapper to
                // define how to serialize/deserialize java objects. The mapper can be customized by adding
                // modules, for example since the Article and Comment object both have Instant fields, the
                // JavaTimeModule is added to provide support for java 8 Time classes, which the mapper itself does
                // not support.
                ObjectMapper mapper = JsonMapper.builder()
                        .addModule(new JavaTimeModule())
                        .build();

                // Create the transport with the Jackson mapper
                ElasticsearchTransport transport = new RestClientTransport(
                        restClient, new JacksonJsonpMapper(mapper));

                // Create the API client
                ElasticsearchClient esClient = new ElasticsearchClient(transport);

                // Creating the indexes
                createIndexWithDateMappingHeadlines(esClient, NEWS_HEADLINES_INDEX_NAME);

                return esClient;
            }

            private void createIndexWithDateMappingHeadlines(ElasticsearchClient esClient, String index) throws IOException {
                synchronized (indexCreationLock) {
                    BooleanResponse indexRes = esClient.indices().exists(ex -> ex.index(index));
                    if (!indexRes.value()) {
                        esClient.indices().create(c -> c
                                .index(index)
                                .mappings(m -> m
                                        .properties("header", p -> p.keyword(d -> d))
                                        .properties("body", p -> p.keyword(d -> d))
                                        .properties("author", p -> p.keyword(d -> d))
                                        .properties("URL", p -> p.keyword(d -> d))
                                        .properties("date", p -> p
                                                .date(d -> d.format("strict_date_optional_time")))
                                ));

                    }
                }
            }
        }

        private boolean newsHeadlineExists(NewsHeadline newsHeadline) throws IOException {
            SearchResponse<NewsHeadline> response = elcClient.search(s -> s
                            .index(NEWS_HEADLINES_INDEX_NAME)
                            .query(q -> q
                                    .match(t -> t
                                            .field("header")
                                            .query(newsHeadline.GetHeader())
                                    )
                            )
                            .query(q -> q
                                    .match(t -> t
                                            .field("author")
                                            .query(newsHeadline.GetAuthor())
                                    )
                            )
                            .query(q -> q
                                    .match(t -> t
                                            .field("URL")
                                            .query(newsHeadline.GetURL())
                                    )
                            ),
                    NewsHeadline.class
            );

            TotalHits total = response.hits().total();

//            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;
//
//            List<Hit<NewsHeadline>> hits = response.hits().hits();
//            for (Hit<NewsHeadline> hit: hits) {
//                NewsHeadline headline = hit.source();
//                logger.indexLog(LOGGER_LEVEL_INFO, "Found product " + headline.GetHeader() + ", score " + hit.score());
//            }

            return total.value() > 0;
        }

        public void consume(String msg) throws IOException {
            logger.indexLog(LOGGER_LEVEL_INFO, Thread.currentThread() + "start");

            try {
                NewsHeadline nh = new NewsHeadline();
                JsonNode newsHeadlineJsonNode = mapper.readTree(msg);

                nh.SetAuthor(newsHeadlineJsonNode.get("author").asText());

                nh.SetBody(newsHeadlineJsonNode.get("body").asText());

                nh.SetHeader(newsHeadlineJsonNode.get("header").asText());

                nh.SetDate(newsHeadlineJsonNode.get("date").asText());

                nh.SetURL(newsHeadlineJsonNode.get("URL").asText());

                if (!newsHeadlineExists(nh)) {
                    IndexRequest<NewsHeadline> indexReq = IndexRequest.of((id -> id
                            .index(NEWS_HEADLINES_INDEX_NAME)
                            .refresh(Refresh.WaitFor)
                            .document(nh)));

                    IndexResponse indexResponse = elcClient.index(indexReq);

                    // Optionally, you can check the index response for success or failure
                    if (indexResponse.result() != null) {
                        // Document indexed successfully
                        logger.indexLog(LOGGER_LEVEL_INFO, "Document indexed successfully!");
                    } else {
                        // Document indexing failed
                        System.err.println("Error occurred during indexing!");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            logger.indexLog(LOGGER_LEVEL_INFO, Thread.currentThread() + "stop");
        }
    }

    public void RunElcConsumer() throws IOException, TimeoutException, InterruptedException {
        Map<String, Document> docVec = java.util.Collections.synchronizedMap(new ConcurrentHashMap<String, Document>());

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        factory.setAutomaticRecoveryEnabled(true);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);

        elcConsumer cons = new elcConsumer(docVec, channel, logger);

        channel.basicConsume(DATA_QUEUE_NAME, false, "javaElcConsumerTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                String message = new String(body, StandardCharsets.UTF_8);
                logger.indexLog(LOGGER_LEVEL_INFO, " Received '" + message + "'  " + Thread.currentThread());
                cons.consume(message);
                channel.basicAck(deliveryTag, false);
            }
        });

        int responceWaitCount = 0;

        final int retryCount = 2;

        while (responceWaitCount<retryCount) {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(DATA_QUEUE_NAME);
            if (response.getMessageCount() != 0) {
                responceWaitCount = 0;
                Thread.sleep(5000);
            } else {
                Thread.sleep(5000);
                responceWaitCount++;
                logger.indexLog(LOGGER_LEVEL_INFO, "Waiting for messages in "+ DATA_QUEUE_NAME +", " + (retryCount-responceWaitCount) * 5 + " seconds until shutdown" + Thread.currentThread());
            }
        }

        try {
            channel.basicCancel("javaElcConsumerTag");
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

}