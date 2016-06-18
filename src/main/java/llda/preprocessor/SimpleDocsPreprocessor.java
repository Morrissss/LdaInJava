package llda.preprocessor;

import llda.model.Model;
import llda.data.Corpus;
import llda.data.Document;
import llda.data.Topics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleDocsPreprocessor {

    private static final Set<String> BLACK_LIST = new HashSet<>(Arrays.asList("小红薯/0ud", "小红薯们/0ud", "上精选/0ud", "可以/v",
                                                                              "队长/n", "薯队长/0ud", "心愿单/0ud",
                                                                              "福利社/n", "小红书/0ud"));

    public SimpleDocsPreprocessor() {
        corpus = new Corpus();
        documents = new ArrayList<>();
        topics = new Topics();
    }

    public Corpus corpus;
    public List<Document> documents;
    public Topics topics;

    private int[] defaultLabels;

    public void loadLabels(int labelNum, Model model) {
        topics = new Topics();
        defaultLabels = new int[labelNum];
        for (int i = 0; i < labelNum; i++) {
            topics.addLabel("topic_" + i);
            defaultLabels[i] = i;
        }
        model.topicNum = topics.size();
    }

    public void loadDocuments(String file, Model model) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s");
                List<Integer> words = new ArrayList<>(parts.length);
                for (int i = 1; i < parts.length; i++) {
                    String token = parts[i];
                    if (token.length() > 1) {
                        String word = token.toLowerCase();
                        if (!BLACK_LIST.contains(word)) {
                            words.add(corpus.addWord(word));
                        }
                    }
                }
                int[] wordIdx = new int[words.size()];
                for (int i = 0; i < words.size(); i++) {
                    wordIdx[i] = words.get(i);
                }
                documents.add(new Document(wordIdx, defaultLabels));
            }
            model.docNum = documents.size();
            model.wordNum = corpus.size();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
