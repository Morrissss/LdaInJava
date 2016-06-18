package llda.model.parallel;

import llda.data.Corpus;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

// one doc with several words
public class Block {
    public int docPartition;
    public int wordPartition;
    public int docIdx;

    /*
     * idx word blockBiasedIdx
     * val word unbiased
     */
    public int[] wordIdx;
    /*
     * idx word blockBiasedIdx
     * val topic biased
     * topic[i] = docStat.candidateTopics[biasedTopics[i]]
     */
    public int[] biasedTopics;

    public DocStat docStat;
    public WordBlockStat wordStat;

    public void setDocStat(DocStat docStat) {
        this.docStat = docStat.clone();
    }
    public void setWordStat(WordBlockStat wordStat) {
        this.wordStat = wordStat.clone();
    }

    public Block(int docPartition, int wordPartition, int docIdx, int[] wordIdx) {
        this.docPartition = docPartition;
        this.wordPartition = wordPartition;
        this.docIdx = docIdx;
        this.wordIdx = wordIdx;
        this.biasedTopics = new int[wordIdx.length];
    }

    public void blockOptimize(double alpha, double beta) {
        double[] sampleProb = new double[docStat.candidateTopics.length];
        for (int blockBiasedWordIdx = 0; blockBiasedWordIdx < wordIdx.length; blockBiasedWordIdx++) {
            int unbiasedWordIdx = wordIdx[blockBiasedWordIdx];
            int statBiasedWordIdx = unbiasedWordIdx - wordStat.wordIdxBeg;
            int biasedOriginTopicIdx = biasedTopics[blockBiasedWordIdx];
            int unbiasedOriginTopicIdx = docStat.candidateTopics[biasedOriginTopicIdx];
            docStat.numWordOnTopic.decrementAndGet(biasedOriginTopicIdx);
            docStat.wordNum -= 1;
            wordStat.numWordOnTopic[unbiasedOriginTopicIdx][statBiasedWordIdx] -= 1;
            wordStat.approxNumWordOnTopic[unbiasedOriginTopicIdx] -= 1;

            int candidateTopicNum = docStat.candidateTopics.length;
            double betaSum = docStat.wordNum * beta;
            // multinomial sampling via cumulative method
            for (int biasedTopicIdx = 0; biasedTopicIdx < candidateTopicNum; biasedTopicIdx++) {
                int unbiasedTopicIdx = docStat.candidateTopics[biasedTopicIdx];
                // 准确值需要除(docStat.blockWordNum + candidateTopicNum * alpha)
                double topicGivenDoc = (docStat.numWordOnTopic.get(biasedTopicIdx) + alpha);
                double wordGivenTopic = (wordStat.numWordOnTopic[unbiasedTopicIdx][statBiasedWordIdx] + beta) /
                                        (wordStat.approxNumWordOnTopic[unbiasedTopicIdx] + betaSum);
                sampleProb[biasedTopicIdx] = topicGivenDoc * wordGivenTopic;
            }
            int biasedNewTopicIdx = sampleProbs(sampleProb);
            int unbiasedNewTopicIdx = docStat.candidateTopics[biasedNewTopicIdx];

            wordStat.approxNumWordOnTopic[unbiasedNewTopicIdx] += 1;
            wordStat.numWordOnTopic[unbiasedNewTopicIdx][statBiasedWordIdx] += 1;
            docStat.wordNum += 1;
            docStat.numWordOnTopic.incrementAndGet(biasedNewTopicIdx);
            biasedTopics[blockBiasedWordIdx] = biasedNewTopicIdx;
        }
    }

    public double logLikelihood() {
        double result = 0;
        for (int biasedWordIdx = 0; biasedWordIdx < wordIdx.length; biasedWordIdx++) {
            double p = 0;
            for (int biasedTopicIdx = 0; biasedTopicIdx < docStat.candidateTopics.length; biasedTopicIdx++) {
                int unbiasedTopicIdx = docStat.candidateTopics[biasedTopicIdx];
                p += docStat.probTopicGivenDoc[biasedTopicIdx] *
                     wordStat.probWordGivenTopic[unbiasedTopicIdx][biasedWordIdx];
            }
            result += Math.log(p);
        }
        return result;
    }

    private int sampleProbs(double[] sampleProb) {
        for (int i = 1; i < sampleProb.length; i++) {
            sampleProb[i] += sampleProb[i-1];
        }
        double selected = ThreadLocalRandom.current().nextDouble(sampleProb[sampleProb.length - 1]);
        int idx = Arrays.binarySearch(sampleProb, selected);
        if (idx < 0) {
            idx = -idx - 1;
        }
        return idx;
    }

    public static void save(List<Block> blocks, Corpus corpus, String dir) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(dir))) {
            for (Block block : blocks) {
                bw.write(block.docIdx + " :");
                for (int i = 0; i < block.wordIdx.length; i++) {
                    bw.write(" " + corpus.word(block.wordIdx[i]) + "@" + block.biasedTopics[i]);
                }
                bw.write("\n");
            }
        }
    }
}
