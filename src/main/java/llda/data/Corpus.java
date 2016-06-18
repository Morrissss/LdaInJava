package llda.data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Corpus {

    private List<String> words;
    private Map<String, Integer> wordIdx;

    public Corpus() {
        this.words = new ArrayList<>();
        this.wordIdx = new HashMap<>();
    }

    public Corpus(List<String> words, Map<String, Integer> wordIdx) {
        this.words = words;
        this.wordIdx = wordIdx;
    }

    public void permute(int[] wordPermute) {
        for (int i = 0; i < words.size(); i++) {
            String word = words.get(i);
            wordIdx.put(word, wordPermute[i]);
        }
        for (Entry<String, Integer> entry : wordIdx.entrySet()) {
            String word = entry.getKey();
            int newIdx = entry.getValue();
            words.set(newIdx, word);
        }
    }

    public int size() {
        return words.size();
    }

    public int idx(String word) {
        return wordIdx.get(word);
    }

    public String word(int idx) {
        return words.get(idx);
    }

    public int addWord(String word) {
        Integer idx = wordIdx.get(word);
        if (idx == null) {
            idx = words.size();
            words.add(word);
            wordIdx.put(word, idx);
        }
        return idx;
    }

    public void save(String dir) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(dir))) {
            for (int i  = 0; i < words.size(); i++) {
                bw.write(i + " : " + words.get(i) + "\n");
            }
        }
    }
}
