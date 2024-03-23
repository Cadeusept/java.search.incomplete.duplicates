package entities;

public class Link {
    private String url;
    private int level;

    public Link(String url, int level) {
        this.url = url;
        this.level = level;
    }

    public String GetUrl() {
        return url;
    }

    public int GetLevel() {
        return level;
    }

    public void SetUrl(String url) {
        this.url = url;
    }

    public void SetLevel(int level) {
        this.level = level;
    }
}
