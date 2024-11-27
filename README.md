
# JAVA INCOMPLETE DUPLICATES FINDER

## Stack
- Java **21**

---

## Work Algorithm of `DuplicateFinder` Class

### 1. Download Texts from Files
- Traverse the specified directory.
- Filter files to include only `.txt` files.
- Read each file’s content and save it with a relative path in a `HashMap`.

---

### 2. Texts Canonization
- Convert all text to lowercase.
- Normalize text to remove diacritical marks.
- Strip all characters except letters, numbers, and spaces.
- Trim and simplify excess whitespace.

---

### 3. Shingles Building
- Split canonicalized text into word tokens.
- Construct shingles (sequences of `k` words) from the tokens.
- Save shingles in a `Set` to represent each file’s unique textual patterns.

---

### 4. Counting MinHashes (Using MurmurHash3)
- Compute `n` hash values for each shingle using the MurmurHash3 algorithm.
- Track the minimum hash value for each hash function across all shingles.
- Store the resulting MinHash vector for each file.

---

### 5. Find Duplicates
- Compare MinHash vectors between all pairs of files.
- Calculate Jaccard similarity based on the percentage of matching hash values in the MinHash vectors.
- Print similarity scores to identify potential duplicates.

---

### Example `Main` Method
```java
package DuplicateFinder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        DuplicateFinder duplicateFinder = new DuplicateFinder();

        // Step 1: Load texts from directory
        String directoryPath = "./input";
        duplicateFinder.loadTexts(directoryPath);

        // Step 2: Canonicalize texts
        duplicateFinder.canonicalizeTexts();

        // Step 3: Build shingles
        int shingleSize = 2; // Define size of shingles
        duplicateFinder.buildShinglesForAllTexts(shingleSize);

        // Step 4: Compute MinHashes
        int hashFunctions = 100; // Number of hash functions
        duplicateFinder.computeMinHashesForAllTexts(hashFunctions);

        // Step 5: Find duplicates
        duplicateFinder.findDuplicates();
    }
}
```

---

### Key Features
- **Text Canonization** ensures noise-free comparisons.
- **Shingling** breaks down text into manageable chunks, allowing for partial matching.
- **MinHashing with MurmurHash3** efficiently calculates similarity while reducing dimensionality.
- **Duplicate Detection** outputs similarity percentages between all file pairs.
