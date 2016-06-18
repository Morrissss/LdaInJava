package llda;

import llda.data.Corpus;
import llda.data.Document;
import llda.data.Topics;
import llda.model.Model;
import llda.model.Partitioner;
import llda.model.Partitioner.PartitionInfo;
import llda.model.parallel.WordBlockStat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ModelTest {

    @Test
    public void testModel() throws Exception {
        int parallelNum = 5;

        Corpus corpus = new Corpus();
        for (int i = 0; i <= 10; i++) {
            corpus.addWord("" + i);
        }
        for (int i = 10; i <= 20; i++) {
            corpus.addWord("" + i);
        }
        for (int i = 20; i <= 30; i++) {
            corpus.addWord("" + i);
        }

        Random random = new Random();
        List<Document> documents = new ArrayList<>();
        int[] topicIdx = new int[]{0, 1, 2};
        for (int i = 0; i < 1000; i++) {
            int type = random.nextInt(3);
            int length = random.nextInt(4) + 2;
            int[] wordIdx = new int[length];
            for (int j = 0; j < wordIdx.length; j++) {
                wordIdx[j] = random.nextInt(10) + type * 10;
            }
            documents.add(new Document(wordIdx, topicIdx));
        }
        Topics topics = new Topics();
        topics.addLabel("0-9");
        topics.addLabel("10-19");
        topics.addLabel("20-29");

        Model model = new Model(10, 100, parallelNum, 10, false);
        model.docNum = documents.size();
        model.wordNum = corpus.size();
        model.topicNum = topics.size();
        model.permute(corpus, documents);
        Partitioner docPartitioner = Partitioner.docsPartitioner(documents, parallelNum);
        Partitioner wordPartitioner = Partitioner.wordsPartitioner(corpus, documents, parallelNum);
        model.split(docPartitioner, corpus, wordPartitioner, documents, topics);
        model.addStats();
        model.initSufficientStatics();
        model.fit();

        for (int unbiasedTopicIdx = 0; unbiasedTopicIdx < topics.size(); unbiasedTopicIdx++) {
            List<Integer> valuesInTopic = new ArrayList<>();
            for (int unbiasedWordIdx = 0; unbiasedWordIdx < corpus.size(); unbiasedWordIdx++) {
                PartitionInfo wordPartition = wordPartitioner.calcPartitionAndLowerBound(unbiasedWordIdx);
                WordBlockStat wordStat = model.wordStats[wordPartition.partition];
                int biasedWordIdx = unbiasedWordIdx - wordStat.wordIdxBeg;
                if (wordStat.probWordGivenTopic[unbiasedTopicIdx][biasedWordIdx] > 0.01) {
                    valuesInTopic.add(Integer.parseInt(corpus.word(unbiasedWordIdx)));
                }
            }
            assertEquals(10, valuesInTopic.size());
            int type = valuesInTopic.get(0) / 10;
            for (int val : valuesInTopic) {
                assertEquals(type, val / 10);
            }
            valuesInTopic.clear();
        }
    }
}
