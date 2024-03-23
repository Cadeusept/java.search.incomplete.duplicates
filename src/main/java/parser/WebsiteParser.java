package parser;

import com.rabbitmq.client.*;
import entities.Link;
import entities.NewsHeadline;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpEntity;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WebsiteParser extends Thread {
    private static final String DATA_QUEUE_NAME = "data_queue";
    private static final String URL_QUEUE_NAME = "url_queue";
    private static final String RMQ_HOST_NAME = "localhost";
    private static final int RMQ_PORT = 5673;
    private static final String RMQ_USERNAME = "rmq_dev";
    private static final String RMQ_PASSWORD = "password";

    private static class linkCatcher {
        private String baseUrl;
        private int depth_count = 1;
        public Deque<Link> urlVec = null;
        public Deque<Link> resultUrlVec = null;
        private Channel rmqChan = null;
        public Scanner cin = new Scanner(System.in);
        public PrintStream pout = new PrintStream(System.out);

        public linkCatcher(int depth, String inputBaseUrl, Channel rmqChannel) throws IOException, TimeoutException {
            urlVec = new ArrayDeque<Link>();
            resultUrlVec = new ArrayDeque<Link>();
            baseUrl = inputBaseUrl;
            urlVec.add(new Link(baseUrl, 0));
            depth_count = depth;
            rmqChan = rmqChannel;
        }

        public void Start() throws IOException {
            for (int i = 0; i < depth_count; ++i) {
                fork();
                pout.println(Thread.currentThread() + "start work");
                urlVec.addAll(resultUrlVec);
            }

            pout.println(Thread.currentThread() + "end work, " + urlVec.size() + "links");
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
                        !link.attr("abs:href").startsWith(baseUrl + "/politics/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/incident/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/culture/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/social/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/economics/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/science/") &&
                                !link.attr("abs:href").startsWith(baseUrl + "/sport/")
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

        // parser.Start();
        linkCatcher prod = new linkCatcher(depth, inputBaseUrl, channel);
        prod.Start();

        channel.close();
        connection.close();
    }

    private static class htmlParser {
        public Scanner cin = new Scanner(System.in);
        public PrintStream pout = new PrintStream(System.out);
        private CloseableHttpClient client = null;
        private Channel rmqChan = null;
        private final int retryCount = 3;
        private final int metadataTimeout = 30 * 1000;
        private final int retryDelay = 5 * 1000;
        private HashMap<String, Document> docVec;

        public htmlParser(HashMap<String, Document> docVec, Channel rmqChannel) throws IOException, TimeoutException {
            rmqChan = rmqChannel;
            this.docVec = docVec;

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
                    pout.println(Thread.currentThread() + "start");
                    response = client.execute(request);
                    pout.println(Thread.currentThread() + "stop");
                    code = response.getStatusLine().getStatusCode();
                    if (code == 404) {
                        pout.println("error get url " + url + " code " + code);
                        try {
                            response.close();
                        } catch (IOException e) {
                            pout.println(e);
                        }
                        // log.warn("error get url " + url + " code " + code);
                        bStop = true;//break;
                    } else if (code == 200) {
                        HttpEntity entity = response.getEntity();
                        if (entity != null) {
                            try {
                                doc = Jsoup.parse(entity.getContent(), "UTF-8", url);
                                docVec.put(url, doc);
                                pout.println(docVec.size() + "docs downloaded");
                                try {
                                    response.close();
                                } catch (IOException e) {
                                    pout.println(e);
                                }
                                break;
                            } catch (IOException e) {
                                // log.error(e);
                            }
                        }
                        bStop = true;
                    } else {
                        pout.println("error get url " + url + " code " + code);
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
                    pout.println(e);
                }
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException e) {
                        pout.println(e);
                    }
                }
            }
        }
    }

    public void RunHtmlParser(HashMap<String, Document> docVec) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DATA_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);

        // parser.Start();
        htmlParser cons = new htmlParser(docVec, channel);

        channel.basicConsume(URL_QUEUE_NAME, false, "javaConsumerTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                // (process the message components here ...)
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'" + "  " + Thread.currentThread());
                cons.parsePage(message);
                channel.basicAck(deliveryTag, false);
            }
        });

        AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(URL_QUEUE_NAME);

        for (;;) {
            if (response.getMessageCount() != 0) {
                Thread.sleep(5000);
            } else {
                break;
            }
        }

        channel.basicCancel("javaConsumerTag");
        channel.close();
        connection.close();
    }

    private static class elkProducer {
        private HashMap<String, Document> docVec = null;
        private Channel rmqChan = null;
        public PrintStream pout = new PrintStream(System.out);
        private final ObjectMapper mapper = new ObjectMapper();

        public elkProducer(HashMap<String, Document> docVec, Channel channel) {
            this.docVec = docVec;
            rmqChan = channel;
        }

        public void ParsePublishNews() {
            for (HashMap.Entry<String, Document> entry : docVec.entrySet()) {
                parsePrintNews(entry.getKey(), entry.getValue());   // dev
                // parseProduceToElk(entry.getKey(), entry.getValue()); // TODO prod
            }
        }

        private void parsePrintNews(String url, Document doc) {
//        Elements spans = doc.select("div [class=article__text__overview]");
//        for (Element element : spans) {
            try {
                pout.println("Header:");
                pout.println(doc.select("h1 [class=article__title]").getFirst().text());
                pout.println("Body:");
                pout.println(doc.select("div [class=article__body]").getFirst().text());
                pout.println("Author:");
                pout.println(doc.select("span [class=article__author-text-link]").getFirst().text());
                pout.println("Date:");
                pout.println(doc.select("time [class=meta__text]").getFirst().text());
                pout.println("URL:");
                pout.println(url);
            } catch (Exception e) {
                pout.println(e);
            }
            //}
        }

        public void parseProduceToElk(String url, Document doc) {
            NewsHeadline newsHeadline = new NewsHeadline();
            //Elements spans = doc.select("div [class=article__text__overview]");
            //for (Element element : spans) {
            try {
                newsHeadline.SetHeader(doc.select("h1 [class=article__title]").getFirst().text());
                newsHeadline.SetBody(doc.select("div [class=article__body]").getFirst().text());
                newsHeadline.SetAuthor(doc.select("span [class=article__author-text-link]").getFirst().text());
                newsHeadline.SetDate(doc.select("time [class=meta__text]").getFirst().text());
                newsHeadline.SetURL(url);
                rmqChan.basicPublish("", DATA_QUEUE_NAME, null, mapper.writeValueAsBytes(newsHeadline));
            } catch (Exception e) {
                // log.error(e);
            }
            //}
        }
    }
    public void RunElkProducer(HashMap<String, Document> docVec) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DATA_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);

        // parser.Start();
        elkProducer prod = new elkProducer(docVec, channel);
        // prod.Start();
        prod.ParsePublishNews();

        channel.close();
        connection.close();
    }
}