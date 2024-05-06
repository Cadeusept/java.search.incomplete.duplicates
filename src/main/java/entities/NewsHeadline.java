package entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

public class NewsHeadline {
    @JsonProperty("id")
    private String id;

    @JsonProperty("header")
    private String header;

    @JsonProperty("body")
    private String body;

    @JsonProperty("author")
    private String author;

    @JsonProperty("date")
    private String date;

    @JsonProperty("URL")
    private String URL;


    public String GetId() {
        return id;
    }
    public String GetHeader() {
        return header;
    }

    public String GetBody() {
        return body;
    }

    public String GetAuthor() {
        return author;
    }

    public String GetDate() {
        return date;
    }

    public String GetURL() {
        return URL;
    }
    public void SetId() throws NoSuchAlgorithmException {
        if (Objects.equals(URL, "")) {
            throw new RuntimeException("hash source is empty");
        }
        byte[] bytesOfMessage = (URL).getBytes(StandardCharsets.UTF_8);
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] theMD5digest = md.digest(bytesOfMessage);
        this.id = Arrays.toString(theMD5digest);
    }
    public void SetHeader(String header) {
        this.header = header;
    }

    public void SetBody(String body) {
        this.body = body;
    }

    public void SetDate(String date) {
        this.date = date;
    }

    public void SetAuthor(String author) {
        this.author = author;
    }

    public void SetURL(String URL) {
        this.URL = URL;
    }
}
