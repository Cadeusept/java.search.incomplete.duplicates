package entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class Log {
    @JsonProperty("message")
    private String message;
    @JsonProperty("level")
    private String level;
    @JsonProperty("created_at")
    private String createdAt;

    public Log(String level, String message) {
        this.message = message;
        this.level = level;
        DateTimeFormatter f = ISODateTimeFormat.dateTime();
        this.createdAt = f.print(LocalDateTime.now());
    }

    public String GetMessage() {
        return message;
    }

    public String GetLevel() {
        return level;
    }
    public String GetCreatedAt() {
        return createdAt;
    }

    public void SetUrl(String url) {
        this.message = url;
    }

    public void SetLevel(String level) {
        this.level = level;
    }
}
