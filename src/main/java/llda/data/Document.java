package llda.data;

import java.util.Arrays;

public class Document {

    public int wordIdx[];    // allows duplicates
    public int candidateTopics[];

    public void permute(int[] wordPermute) {
        for (int i = 0; i < wordIdx.length; i++) {
            wordIdx[i] = wordPermute[wordIdx[i]];
        }
        Arrays.sort(wordIdx);
    }

    public String originStr(Corpus corpus) {
        StringBuilder sb = new StringBuilder();
        for (int i : wordIdx) {
            sb.append(corpus.word(i) + " ");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String labelStr(Topics topics) {
        StringBuilder sb = new StringBuilder();
        if (candidateTopics != null) {
            for (int i : candidateTopics) {
                sb.append(topics.label(i) + " ");
            }
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public int length() {
        return wordIdx.length;
    }

    /**
     * not empty, not null
     */
    public Document(int[] wordIdx, int[] candidateTopics) {
        this.wordIdx = wordIdx;
        this.candidateTopics = candidateTopics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Document{");
        sb.append("wordIdx=").append(Arrays.toString(wordIdx));
        sb.append(", candidateTopics=").append(Arrays.toString(candidateTopics));
        sb.append('}');
        return sb.toString();
    }
}
