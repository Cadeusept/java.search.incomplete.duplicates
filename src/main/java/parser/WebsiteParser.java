package parser;

import com.rabbitmq.client.*;
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
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpEntity;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WebsiteParser extends Thread {
    private String baseUrl;
    private int depth_count = 1;
    public Deque<String> urlVec = null;
    public Deque<String> resultUrlVec = null;
    public Scanner cin = new Scanner(System.in);
    public PrintStream pout = new PrintStream(System.out);
    private CloseableHttpClient client = null;
    private static final String DATA_QUEUE_NAME = "data_queue";
    private Channel rmqChan = null;
    private final int retryCount = 3;
    private final int metadataTimeout = 30 * 1000;
    private final int retryDelay = 5 * 1000;
    private HashMap<String, Document> docVec;
    private final ObjectMapper mapper = new ObjectMapper();


    public static void runProducer(int depth, String inputBaseUrl) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5673);
        factory.setUsername("mq_dev");
        factory.setPassword("dev_password");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        WebsiteParser parser = new WebsiteParser(depth, inputBaseUrl, channel);

        channel.queueDeclare(DATA_QUEUE_NAME, false, false, true, null);
        channel.basicQos(1);

        parser.Start();

        channel.close();
        connection.close();
    }

    public WebsiteParser(int depth, String inputBaseUrl, Channel rmqChannel) throws IOException, TimeoutException {
        urlVec = new ArrayDeque<String>();
        resultUrlVec = new ArrayDeque<String>();
        docVec = new HashMap<String, Document>();
        depth_count = depth;
        baseUrl = inputBaseUrl;
        urlVec.add(baseUrl);
        rmqChan = rmqChannel;

//        CookieStore cookieStore = new BasicCookieStore();
        client = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .setDefaultCookieStore(new BasicCookieStore()).build();
    }

    public void Start() throws IOException {
        for (int i = 0; i < depth_count; ++i) {
            fork();
            // pout.println(Thread.currentThread() + "start work");
            urlVec.addAll(resultUrlVec);
        }

        // pout.println(Thread.currentThread() + "end work");
    }

    private void fork() throws IOException {
        String cur;

        while ((cur = urlVec.pollFirst()) != null) {
            parseUrl(cur);
        }
    }

    private void parseUrl(String url) throws IOException {
        Document doc = Jsoup.connect(url).get();
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

            parsePage(link.attr("abs:href"));

            // rmqChan.basicPublish("", URL_QUEUE_NAME, null, link.attr("abs:href").getBytes(StandardCharsets.UTF_8));
            resultUrlVec.add(link.attr("abs:href"));
        }
    }

    private void parsePage(String url) throws IOException {
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
                    // log.warn("error get url " + url + " code " + code);
                    bStop = true;//break;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            doc = Jsoup.parse(entity.getContent(), "UTF-8", url);
                            docVec.put(url, doc);
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
                // log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // log.error(e);
                }
            }
        }
    }

    private void parseProduceNews() {
        for (HashMap.Entry<String, Document> entry : docVec.entrySet()) {
            parsePrintNews(entry.getKey(), entry.getValue());

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
                // log.error(e);
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

    //class="news-speeches_wrap items_data"
}