package llda.model.parallel;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;

public class SampleCaller implements Callable<Double> {

    private int docPartition;
    private int wordPartition;
    private CountDownLatch latch;

    private List<Block> blocks;
    private int[] currentNumWordOnTopic;

    private double alpha;
    private double beta;
    private int nIter;

    public SampleCaller(int parallelIdx, int parallelNum, int docPartition, CountDownLatch latch,
                        List<Block> blocks, int[] currentNumWordOnTopic,
                        double alpha, double beta, int batchIterNum) {
        this.docPartition = docPartition;
        this.wordPartition = (docPartition + parallelIdx) % parallelNum;
        this.latch = latch;
        this.blocks = blocks.stream().filter(b -> b.docPartition == this.docPartition &&
                                                  b.wordPartition == this.wordPartition)
                            .collect(Collectors.toList());
        this.currentNumWordOnTopic = currentNumWordOnTopic;
        this.alpha = alpha;
        this.beta = beta;
        this.nIter = batchIterNum;
    }

    @Override
    public Double call() {
        try {
            for (Block block : blocks) {
                block.wordStat.approxNumWordOnTopic = Arrays.copyOf(currentNumWordOnTopic, currentNumWordOnTopic.length);
                break;
            }
            for (int iter = 0; iter < nIter; iter++) {
                for (Block block : blocks) {
                    block.blockOptimize(alpha, beta);
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
