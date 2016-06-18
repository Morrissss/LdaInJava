package llda.model;

import llda.Pair;
import llda.model.parallel.Block;
import llda.model.parallel.DocStat;
import llda.model.parallel.InitCaller;
import llda.model.parallel.SampleCaller;
import llda.model.parallel.WordBlockStat;
import llda.model.Partitioner.PartitionInfo;
import llda.data.Corpus;
import llda.data.Document;
import llda.data.Topics;
import llda.preprocessor.SimpleDocsPreprocessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Model {

    public double alpha = 0.05; // LDA hyper-parameters
    public double beta = 0.01;  // LDA hyper-parameters
    public int burnInNum;
    public int iterNum;

    public int topicNum;
    public int docNum;
    public int wordNum;

    public int parallelNum;
    public int batchSize;

    public List<Block> blocks;
    public DocStat[] docStats;
    public WordBlockStat[] wordStats;
    public int[] globalNumWordOnTopics;

    private boolean log;
    private int updateNum;
    private ExecutorService executor;

    public Model(int burnInNum, int iterNum, int parallelNum, int batchSize, boolean log) {
        this.burnInNum = burnInNum;
        this.iterNum = iterNum;
        this.parallelNum = parallelNum;
        this.batchSize = batchSize;
        this.log = log;
        this.updateNum = 0;
        this.executor = Executors.newFixedThreadPool(parallelNum);
    }

    public void permute(Corpus corpus, List<Document> documents) {
        Partitioner.permuteList(documents);
        int[] wordPermute = Partitioner.permuteArray(corpus.size());
        corpus.permute(wordPermute);
        for (Document doc : documents) {
            doc.permute(wordPermute);
        }
    }

    // docsPartitioner.getPartNum() == wordsPartitioner.getPartNum()
    public void split(Partitioner docsPartitioner, Corpus corpus,
                      Partitioner wordsPartitioner, List<Document> documents,
                      Topics topics) {
        int parallelNum = docsPartitioner.getPartNum();

        docStats = new DocStat[docNum];
        wordStats = new WordBlockStat[parallelNum];
        blocks = new ArrayList<>();

        for (int docIdx = 0; docIdx < documents.size(); docIdx++) {
            Document doc = documents.get(docIdx);
            PartitionInfo docPartitionInfo = docsPartitioner.calcPartitionAndLowerBound(docIdx);
            docStats[docIdx] = new DocStat(docPartitionInfo.partition, docIdx, doc.wordIdx.length, doc.candidateTopics);
            int lastWordPartition = -1;
            List<Integer> words = new ArrayList<>();
            for (int wordIdx : doc.wordIdx) {
                PartitionInfo wordPartitionInfo = wordsPartitioner.calcPartitionAndLowerBound(wordIdx);
                int currentWordPartition = wordPartitionInfo.partition;
                if (wordStats[currentWordPartition] == null) {
                    wordStats[currentWordPartition] = new WordBlockStat(wordPartitionInfo.partition,
                                                                        wordPartitionInfo.partitionLowerBound,
                                                                        wordPartitionInfo.partitionSize,
                                                                        topics.size());
                }
                if (lastWordPartition >= 0 && currentWordPartition != lastWordPartition) {
                    Block block = new Block(docPartitionInfo.partition, lastWordPartition,
                                            docIdx, words.stream().mapToInt(Integer::intValue).toArray());
                    if (block.wordIdx.length > 0) {
                        blocks.add(block);
                    }
                    words.clear();
                }
                words.add(wordIdx);
                lastWordPartition = currentWordPartition;
            }
            Block block = new Block(docPartitionInfo.partition, lastWordPartition,
                                    docIdx, words.stream().mapToInt(Integer::intValue).toArray());
            if (block.wordIdx.length > 0) {
                blocks.add(block);
            }
        }
    }

    public void addStats() {
        for (Block block : blocks) {
            block.docStat = docStats[block.docIdx];
            block.wordStat = wordStats[block.wordPartition];
        }
    }

    public void initSufficientStatics() throws Exception {
        globalNumWordOnTopics = new int[topicNum];    // init as zeros
        for (int parallelIdx = 0; parallelIdx < parallelNum; parallelIdx++) {
            List<Future<Double>> approxNumWordOnTopics = new ArrayList<>(parallelNum);
            CountDownLatch latch = new CountDownLatch(parallelNum);
            for (int docPartition = 0; docPartition < parallelNum; docPartition++) {
                approxNumWordOnTopics.add(executor.submit(new InitCaller(parallelIdx, parallelNum, docPartition,
                                                                         latch, blocks, globalNumWordOnTopics)));
            }
            latch.await();
            globalNumWordOnTopics = mergeNumWordOnTopic(globalNumWordOnTopics);
        }
    }

    private int[] mergeNumWordOnTopic(int[] lastNumWordOnTopic) {
        int[] result = Arrays.copyOf(lastNumWordOnTopic, topicNum);
        for (WordBlockStat wordStat : wordStats) {
            for (int i = 0; i < topicNum; i++) {
                result[i] += wordStat.approxNumWordOnTopic[i] - lastNumWordOnTopic[i];
            }
        }
        return result;
    }

    public void fit() throws Exception {
        updateNum = 0;
        for (int iter = 0; iter < iterNum; iter++) {
            long begTime = System.currentTimeMillis();
            for (int parallelIdx = 0; parallelIdx < parallelNum; parallelIdx++) {
                List<Future<Double>> approxNumWordOnTopics = new ArrayList<>(parallelNum);
                CountDownLatch latch = new CountDownLatch(parallelNum);
                for (int docPartition = 0; docPartition < parallelNum; docPartition++) {
                    approxNumWordOnTopics.add(executor.submit(new SampleCaller(parallelIdx, parallelNum, docPartition,
                                                                               latch, blocks, globalNumWordOnTopics,
                                                                               alpha, beta, batchSize)));
                }
                latch.await();
                globalNumWordOnTopics = mergeNumWordOnTopic(globalNumWordOnTopics);
            }
            if (log) {
                System.out.println("Sample: " + (System.currentTimeMillis() - begTime) / 1000);
            }
            if (iter >= burnInNum) {
                for (DocStat docStat : docStats) {
                    docStat.updateProbTopicGivenDoc(alpha, updateNum);
                }
                for (WordBlockStat wordStat : wordStats) {
                    wordStat.updateProbWordGivenTopic(beta, updateNum, globalNumWordOnTopics);
                }
                updateNum++;
                if (log && iter % 10 == 0) {
                    System.out.println("Count: " + Arrays.toString(globalNumWordOnTopics));
                    System.out.println("===========================" + iter + " : " + logLikelihood());
                }
            }
        }
    }

    public double logLikelihood() {
        return blocks.stream().mapToDouble(block -> block.logLikelihood()).sum();
    }

    public static void main(String[] args) throws Exception {
        String dir = args[0];
        int iterNum = Integer.parseInt(args[1]);
        int labelNum = Integer.parseInt(args[2]);
        int burnInNum = Integer.parseInt(args[3]);
        int batchIterNum = Integer.parseInt(args[4]);
        int parallelNum = 10;
        Model model = new Model(burnInNum, iterNum, parallelNum, batchIterNum, true);
        SimpleDocsPreprocessor preprocessor = new SimpleDocsPreprocessor();
        preprocessor.loadLabels(labelNum, model);
        preprocessor.loadDocuments(dir, model);

        model.permute(preprocessor.corpus, preprocessor.documents);
        preprocessor.corpus.save("corpus.txt");
        Partitioner docPartitioner = Partitioner.docsPartitioner(preprocessor.documents, parallelNum);
        Partitioner wordPartitioner = Partitioner.wordsPartitioner(preprocessor.corpus, preprocessor.documents, parallelNum);
        model.split(docPartitioner, preprocessor.corpus, wordPartitioner, preprocessor.documents, preprocessor.topics);
        model.addStats();
        model.initSufficientStatics();
        model.fit();

        int wordNum = preprocessor.corpus.size();
        int topicNum = preprocessor.topics.size();
        for (int unbiasedTopicIdx = 0; unbiasedTopicIdx < topicNum; unbiasedTopicIdx++) {
            List<Pair<String, Double>> wordProb = new ArrayList<>(wordNum);
            for (int unbiasedWordIdx = 0; unbiasedWordIdx < wordNum; unbiasedWordIdx++) {
                PartitionInfo wordPartitionInfo = wordPartitioner.calcPartitionAndLowerBound(unbiasedWordIdx);
                int biasedWordIdx = unbiasedWordIdx - wordPartitionInfo.partitionLowerBound;
                wordProb.add(Pair.of(preprocessor.corpus.word(unbiasedWordIdx),
                                     model.wordStats[wordPartitionInfo.partition].probWordGivenTopic[unbiasedTopicIdx][biasedWordIdx]));
            }
            Collections.sort(wordProb, (p1, p2) -> Double.compare(p2.second, p1.second));
            for (int j = 0; j < 50; j++) {
                System.out.println(unbiasedTopicIdx + ": " + wordProb.get(j));
            }
        }
        Block.save(model.blocks, preprocessor.corpus, "blocks.txt");
        DocStat.save(model.docStats, preprocessor.corpus, preprocessor.documents, "docStats.txt");
        WordBlockStat.save(model.wordStats, preprocessor.corpus, "wordBlockStats.txt");
        preprocessor.corpus.save("corpus.txt");
        System.out.println("DONE");
    }
}
