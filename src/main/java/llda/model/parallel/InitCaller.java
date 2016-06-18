package llda.model.parallel;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;

public class InitCaller implements Callable<Double> {

    private int docPartition;
    private int wordPartition;
    private CountDownLatch latch;
    private List<Block> blocks;
    private int[] currentNumWordOnTopic;

    public InitCaller(int parallelIdx, int parallelNum, int docPartition, CountDownLatch latch,
                      List<Block> blocks, int[] currentNumWordOnTopic) {
        this.docPartition = docPartition;
        this.wordPartition = (docPartition + parallelIdx) % parallelNum;
        this.latch = latch;
        this.blocks = blocks.stream().filter(b -> b.docPartition == this.docPartition &&
                                                  b.wordPartition == this.wordPartition)
                            .collect(Collectors.toList());
        this.currentNumWordOnTopic = currentNumWordOnTopic;
    }

    @Override
    public Double call() throws Exception {
        try {
            for (Block block : blocks) {
                block.wordStat.approxNumWordOnTopic = Arrays.copyOf(currentNumWordOnTopic, currentNumWordOnTopic.length);
                break;
            }
            for (Block block : blocks) {
                for (int blockBiasedWordIdx = 0; blockBiasedWordIdx < block.wordIdx.length; blockBiasedWordIdx++) {
                    int unbiasedWordIdx = block.wordIdx[blockBiasedWordIdx];
                    int statBiasedWordIdx = unbiasedWordIdx - block.wordStat.wordIdxBeg;
                    int biasedTopicIdx = ThreadLocalRandom.current().nextInt(block.docStat.candidateTopics.length);
                    int unbiasedTopicIdx = block.docStat.candidateTopics[biasedTopicIdx];

                    block.biasedTopics[blockBiasedWordIdx] = biasedTopicIdx;
                    block.docStat.numWordOnTopic.incrementAndGet(biasedTopicIdx);
                    block.wordStat.numWordOnTopic[unbiasedTopicIdx][statBiasedWordIdx] += 1;
                    block.wordStat.approxNumWordOnTopic[unbiasedTopicIdx] += 1;
                }
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            latch.countDown();
        }
    }
}
