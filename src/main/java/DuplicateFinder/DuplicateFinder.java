package DuplicateFinder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.*;
import java.util.stream.Stream;

import Entities.FileData;
import org.apache.commons.codec.digest.MurmurHash3;

public class DuplicateFinder {
    // Единая хэш-таблица: ключ — относительный путь, значение — данные файла
    private final Map<String, FileData> fileDataMap;

    public DuplicateFinder() {
        this.fileDataMap = new HashMap<>();
    }

    // Загрузка текстов из файлов
    public void loadTexts(String directoryPath) {
        Path basePath = Paths.get(directoryPath); // Базовая директория

        // нужно для автоматического закрытия потока
        try (Stream<Path> paths = Files.walk(basePath)) {
            paths.filter(Files::isRegularFile) // Фильтруем только обычные файлы
                    .filter(path -> path.toString().endsWith(".txt")) // Фильтруем только .txt файлы
                    .forEach(path -> {
                        try {
                            String relativePath = basePath.relativize(path).toString(); // Относительный путь
                            String content = Files.readString(path); // Чтение содержимого файла
                            fileDataMap.put(relativePath, new FileData(content)); // Сохраняем данные
                        } catch (IOException e) {
                            System.err.printf("Ошибка чтения файла: %s%n", path);
                        }
                    });
        } catch (IOException e) {
            System.err.println("Ошибка при обходе файловой системы: " + e.getMessage());
        }
    }

    // Канонизация текста
    private static String canonicalize(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }

        // Приведение текста к нижнему регистру
        text = text.toLowerCase();

        // Нормализация текста (удаление диакритических знаков)
        text = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("\\p{M}", "");

        // Удаление всех символов, кроме букв, цифр и пробелов
        text = text.replaceAll("[^\\p{L}\\p{N}\\s]", " ");

        // Упрощение пробелов и обрезка
        text = text.replaceAll("\\s+", " ").trim();

        return text;
    }

    // Канонизация текстов в хэш-таблице
    public void canonicalizeTexts() {
        for (FileData fileData : fileDataMap.values()) {
            String canonizedText = canonicalize(fileData.getText());
            fileData.setText(canonizedText);
        }
    }

    // Построение шинглов
    public void buildShinglesForAllTexts(int shingleSize) {
        for (Map.Entry<String, FileData> entry : fileDataMap.entrySet()) {
            FileData fileData = entry.getValue();
            Set<String> shingles = buildShingles(fileData.getText(), shingleSize);
            fileData.setShingles(shingles);
        }
    }

    // Построение шинглов для конкретного текста
    private Set<String> buildShingles(String text, int shingleSize) {
        Set<String> shingles = new HashSet<>();
        String[] words = text.split(" ");
        for (int i = 0; i <= words.length - shingleSize; i++) {
            shingles.add(String.join(" ", Arrays.copyOfRange(words, i, i + shingleSize)));
        }
        return shingles;
    }

    // Вычисление MinHash
    public void computeMinHashesForAllTexts(int numHashes) {
        for (Map.Entry<String, FileData> entry : fileDataMap.entrySet()) {
            FileData fileData = entry.getValue();
            Set<String> shingles = fileData.getShingles();
            int[] minHash = computeMinHash(shingles, numHashes);
            fileData.setMinHash(minHash);
        }
    }

    // Вычисление MinHash для множества шинглов
    private int[] computeMinHash(Set<String> shingles, int numHashes) {
        int[] minHashes = new int[numHashes];
        Arrays.fill(minHashes, Integer.MAX_VALUE);

        for (String shingle : shingles) {
            byte[] shingleBytes = shingle.getBytes();
            for (int i = 0; i < numHashes; i++) {
                int hash = MurmurHash3.hash32x86(shingleBytes, 0, shingleBytes.length, i);
                minHashes[i] = Math.min(minHashes[i], hash);
            }
        }

        return minHashes;
    }

    // Сравнение MinHash и вывод схожести
    public void compareHashes() {
        List<String> relativePaths = new ArrayList<>(fileDataMap.keySet());
        for (int i = 0; i < relativePaths.size(); i++) {
            for (int j = i + 1; j < relativePaths.size(); j++) {
                String path1 = relativePaths.get(i);
                String path2 = relativePaths.get(j);
                double similarity = calculateSimilarity(
                        fileDataMap.get(path1).getMinHash(),
                        fileDataMap.get(path2).getMinHash()
                );
                System.out.printf("Similarity between \"%s\" and \"%s\": %.2f%%%n", path1, path2, similarity * 100);
            }
        }
    }

    // Вычисление схожести между MinHash-векторами
    private double calculateSimilarity(int[] hash1, int[] hash2) {
        int count = 0;
        for (int i = 0; i < hash1.length; i++) {
            if (hash1[i] == hash2[i]) {
                count++;
            }
        }
        return (double) count / hash1.length;
    }

    // Главный метод для выполнения поиска дубликатов
    public void findDuplicates() {
        compareHashes();
    }
}