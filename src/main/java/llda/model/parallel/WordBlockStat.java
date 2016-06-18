package llda.model.parallel;

import llda.data.Corpus;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;

// several words
public class WordBlockStat {
    public int wordPartition;

    public int wordIdxBeg;
    public int blockWordNum;
    /*
     * idx topic: unbiased
     * idx word: statBiased
     * probTopicGivenDoc[i][j] = P(word[j+wordIdxBeg] | topics[i])
     * globalTopicNum * blockWordNum
     */
    public double[][] probWordGivenTopic;
    /*
     * idx topic: unbiased
     * idx word: statBiased
     * numWordOnTopic[i][j] = #(word[j+wordIdxBeg], topics[i])
     * globalTopicNum * blockWordNum
     */
    public int[][] numWordOnTopic;
    /*
     * idx topic unbiased
     */
    public int[] approxNumWordOnTopic;

    public WordBlockStat(int wordPartition, int wordIdxBeg, int blockWordNum, int globalTopicNum) {
        this.wordPartition = wordPartition;
        this.wordIdxBeg = wordIdxBeg;
        this.blockWordNum = blockWordNum;
        this.probWordGivenTopic = new double[globalTopicNum][blockWordNum];
        this.numWordOnTopic = new int[globalTopicNum][];
        for (int i = 0; i < this.numWordOnTopic.length; i++) {
            this.numWordOnTopic[i] = new int[blockWordNum];
        }
    }

    public void updateProbWordGivenTopic(double beta, int alreadyUpdateNum, int[] globalNumWordOnTopics) {
        double betaSum = blockWordNum * beta;
        for (int unbiasedTopicIdx = 0; unbiasedTopicIdx < probWordGivenTopic.length; unbiasedTopicIdx++) {
            for (int statBiasedWordIdx = 0; statBiasedWordIdx < blockWordNum; statBiasedWordIdx++) {
                double oldPart = probWordGivenTopic[unbiasedTopicIdx][statBiasedWordIdx];
                double newPart = (numWordOnTopic[unbiasedTopicIdx][statBiasedWordIdx] + beta) /
                                 (globalNumWordOnTopics[unbiasedTopicIdx] + betaSum);
                probWordGivenTopic[unbiasedTopicIdx][statBiasedWordIdx] = (alreadyUpdateNum * oldPart + newPart) /
                                                                          (alreadyUpdateNum + 1);
            }
        }
    }

    @Override
    public WordBlockStat clone() {
        try {
            WordBlockStat result = (WordBlockStat) super.clone();
            result.probWordGivenTopic = new double[this.probWordGivenTopic.length][];
            for (int i = 0; i < this.probWordGivenTopic.length; i++) {
                result.probWordGivenTopic[i] = Arrays.copyOf(this.probWordGivenTopic[i], this.probWordGivenTopic.length);
            }
            result.numWordOnTopic = new int[this.numWordOnTopic.length][];
            for (int i = 0; i < this.numWordOnTopic.length; i++) {
                result.numWordOnTopic[i] = Arrays.copyOf(this.numWordOnTopic[i], this.numWordOnTopic[i].length);
            }
            if (this.approxNumWordOnTopic != null) {
                result.approxNumWordOnTopic = Arrays.copyOf(this.approxNumWordOnTopic, this.approxNumWordOnTopic.length);
            }
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public static void save(WordBlockStat[] wordStats, Corpus corpus, String dir) throws Exception {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(dir))) {
            for (WordBlockStat wordStat : wordStats) {
                for (int i = 0; i < wordStat.blockWordNum; i++) {
                    int unbiasedWordIdx = wordStat.wordIdxBeg + i;
                    bw.write(unbiasedWordIdx + " : " + corpus.word(unbiasedWordIdx) + " = ");
                    for (int t = 0; t < wordStat.numWordOnTopic.length; t++) {
                        bw.write(" " + wordStat.probWordGivenTopic[t][i] + "@" + wordStat.numWordOnTopic[t][i]);
                    }
                    bw.write("\n");
                }
            }
        }
    }
}
