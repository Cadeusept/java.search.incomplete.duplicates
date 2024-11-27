package DuplicateFinder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        DuplicateFinder duplicateFinder = new DuplicateFinder();

        // 1. Загрузка текстов из файлов
        String directoryPath = "./input";
        duplicateFinder.loadTexts(directoryPath);

        // 2. Канонизация текстов
        duplicateFinder.canonicalizeTexts();

        // 3. Построение шинглов
        int shingleSize = 2;
        duplicateFinder.buildShinglesForAllTexts(shingleSize);

        // 4. Вычисление MinHash
        int hashFunctions = 100;
        duplicateFinder.computeMinHashesForAllTexts(hashFunctions);

        // 5. Поиск дубликатов
        duplicateFinder.findDuplicates();
    }
}