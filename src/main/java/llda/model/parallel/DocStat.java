package llda.model.parallel;

import llda.data.Corpus;
import llda.data.Document;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

// one doc
public class DocStat {
    public int docPartition;

    public int docIdx;
    public int wordNum;
    /*
     * idx topic biased
     * val topic unbiased
     */
    public int[] candidateTopics;
    /*
     * val topic biased
     * numWordOnTopic[i] = #(Block.biasedTopics[x] == i, doc[docIdx])
     */
    public AtomicIntegerArray numWordOnTopic;
    /*
     * idx topic biased
     * probTopicGivenDoc[i] = P(candidateTopics[i] | doc[docIdx])
     */
    public double[] probTopicGivenDoc;

    public DocStat(int docPartition, int docIdx, int wordNum, int[] candidateTopics) {
        this.docPartition = docPartition;
        this.docIdx = docIdx;
        this.wordNum = wordNum;
        this.candidateTopics = candidateTopics;
        this.numWordOnTopic = new AtomicIntegerArray(candidateTopics.length);
        this.probTopicGivenDoc = new double[candidateTopics.length];
    }

    public void updateProbTopicGivenDoc(double alpha, int alreadyUpdateNum) {
        double alphaSum = candidateTopics.length * alpha;
        for (int biasedTopicIdx = 0; biasedTopicIdx < candidateTopics.length; biasedTopicIdx++) {
            double oldPart = probTopicGivenDoc[biasedTopicIdx];
            double newPart = (numWordOnTopic.get(biasedTopicIdx) + alpha) / (wordNum + alphaSum);
            probTopicGivenDoc[biasedTopicIdx] = (alreadyUpdateNum * oldPart + newPart) / (alreadyUpdateNum + 1);
        }
    }

    @Override
    public DocStat clone() {
        try {
            DocStat result = (DocStat) super.clone();
            result.candidateTopics = Arrays.copyOf(this.candidateTopics, this.candidateTopics.length);
            result.numWordOnTopic = new AtomicIntegerArray(this.numWordOnTopic.length());
            for (int i = 0; i < this.numWordOnTopic.length(); i++) {
                result.numWordOnTopic.set(i, this.numWordOnTopic.get(i));
            }
            result.probTopicGivenDoc = Arrays.copyOf(this.probTopicGivenDoc, this.probTopicGivenDoc.length);
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public static void save(DocStat[] docStats, Corpus corpus, List<Document> documents, String dir) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(dir))) {
            for (DocStat doc : docStats) {
                bw.write(doc.docIdx + " :");
                for (int idx : documents.get(doc.docIdx).wordIdx) {
                    bw.write(" " + corpus.word(idx));
                }
                bw.write(" === ");
                for (int i = 0; i < doc.numWordOnTopic.length(); i++) {
                    bw.write(doc.probTopicGivenDoc[i] + "#" + doc.numWordOnTopic.get(i) + " ");
                }
                bw.write("\n");
            }
        }
    }
}
