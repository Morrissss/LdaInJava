package llda.model;

import llda.data.Corpus;
import llda.data.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Partitioner {

    public static class PartitionInfo {
        public final int partition;
        public final int partitionLowerBound;
        public final int partitionSize;
        public PartitionInfo(int partition, int partitionLowerBound, int partitionSize) {
            this.partition = partition;
            this.partitionLowerBound = partitionLowerBound;
            this.partitionSize = partitionSize;
        }
    }

    private List<Integer> partitionUpperIdx;
    private int partNum;
    private Partitioner(List<Integer> partitionUpperIdx, int partNum) {
        this.partitionUpperIdx = partitionUpperIdx;
        this.partNum = partNum;
    }

    public static void randomShift(Corpus corpus, List<Document> documents) {
        permuteList(documents);

        int[] wordPermute = permuteArray(corpus.size());
        corpus.permute(wordPermute);
        for (Document doc : documents) {
            doc.permute(wordPermute);
        }
    }

    // partNum should not be so large as comparable to corpus.size() or documents.size()
    public static Partitioner wordsPartitioner(Corpus corpus, List<Document> documents, int partNum) {
        int total = 0;
        int[] docNum = new int[corpus.size()];
        for (int i = 0; i < corpus.size(); i++) {
            docNum[i] = 0;
        }
        for (Document doc : documents) {
            for (int w : doc.wordIdx) {
                docNum[w] += 1;
                total += 1;
            }
        }
        int partSize = total / partNum + 1;
        List<Integer> partitionWordIdx = new ArrayList<>(partNum);
        int aggr = 0;
        for (int i = 0; i < docNum.length; i++) {
            aggr += docNum[i];
            if (aggr > (partitionWordIdx.size()+1)*partSize) {
                partitionWordIdx.add(i);
            }
        }
        partitionWordIdx.add(docNum.length);
        if (partitionWordIdx.size() != partNum) {
            throw new IllegalArgumentException("Illegal partNum");
        }
        return new Partitioner(partitionWordIdx, partNum);
    }

    public static Partitioner docsPartitioner(List<Document> documents, int partNum) {
        List<Integer> partitionDocIdx = new ArrayList<>(partNum);
        int partSize = documents.size() / partNum + 1;
        for (int i = 1; i < partNum; i++) {
            partitionDocIdx.add(i * partSize);
        }
        partitionDocIdx.add(documents.size());
        if (partitionDocIdx.size() != partNum) {
            throw new IllegalArgumentException("Illegal partNum");
        }
        return new Partitioner(partitionDocIdx, partNum);
    }

    public PartitionInfo calcPartitionAndLowerBound(int idx) {
        int partition = Collections.binarySearch(partitionUpperIdx, idx);
        if (partition < 0) {
            // must be less than partitionUpperIdx.size(), since partitionUpperIdx[len] = partitionUpperIdx.size()
            partition = -partition - 1;
        } else {
            partition += 1;
        }
        int lowerBound = 0;
        if (partition > 0) {
            lowerBound = partitionUpperIdx.get(partition-1);
        }
        int size = partitionUpperIdx.get(partition) - lowerBound;
        return new PartitionInfo(partition, lowerBound, size);
    }

    public int getPartNum() {
        return this.partNum;
    }

    static <T> void permuteList(List<T> list) {
        Random random = new Random();
        int len = list.size();
        for (int i = 0; i < len; i++) {
            Collections.swap(list, i, random.nextInt(len - i) + i);
        }
    }

    static int[] permuteArray(int len) {
        int[] result = new int[len];
        for (int i = 0; i < len; i++) {
            result[i] = i;
        }
        Random random = new Random();
        for (int i = 0; i < len; i++) {
            arraySwap(result, i, random.nextInt(len - i) + i);
        }
        return result;
    }

    private static void arraySwap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}
