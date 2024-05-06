package entities;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Link {
    private String id;
    private String url;
    private int level;

    public Link(String url, int level) throws NoSuchAlgorithmException {
        byte[] bytesOfMessage = (url).getBytes(StandardCharsets.UTF_8);
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] theMD5digest = md.digest(bytesOfMessage);
        this.id = Arrays.toString(theMD5digest);
        this.url = url;
        this.level = level;
    }

    public String GetId() {
        return id;
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
