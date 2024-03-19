package entities;

import java.util.Date;

public class NewsHeadline {
    private String header;
    private String body;
    private String author;
    private String date;
    private String URL;

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
