package Entities;

import java.util.Set;

public class FileData {
    private String text;           // Исходный или канонизированный текст
    private Set<String> shingles;  // Шинглы текста
    private int[] minHash;         // MinHash для текста

    public FileData(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Set<String> getShingles() {
        return shingles;
    }

    public void setShingles(Set<String> shingles) {
        this.shingles = shingles;
    }

    public int[] getMinHash() {
        return minHash;
    }

    public void setMinHash(int[] minHash) {
        this.minHash = minHash;
    }
}
