package llda.model;

import llda.model.Partitioner;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class PartitionerTest {

    @Test
    public void testPermuteArray() {
        int arrDim = 10;
        int nSample = 1000000;
        int rowSum = (arrDim-1) * arrDim / 2;

        double colSum = nSample * (arrDim-1) / 2;
        double[] colAcc = new double[arrDim];

        for (int c = 0; c < nSample; c++) {
            int[] arr = Partitioner.permuteArray(arrDim);

            int s = 0;
            for (int i = 0; i < arrDim; i++) {
                colAcc[i] += arr[i];
                s += arr[i];
            }
            assertEquals(rowSum, s);
        }
        for (int c = 0; c < arrDim; c++) {
            assertEquals(colSum, colAcc[c], nSample/100.0);
        }
    }

    @Test
    public void testPermuteList() {
        int listDim = 10;
        int nSample = 1000000;
        int rowSum = (listDim-1) * listDim / 2;

        double colSum = nSample * (listDim-1) / 2;
        double[] colAcc = new double[listDim];

        for (int c = 0; c < nSample; c++) {
            List<Integer> list = IntStream.range(0, listDim).boxed().collect(Collectors.toList());
            Partitioner.permuteList(list);

            int s = 0;
            for (int i = 0; i < listDim; i++) {
                colAcc[i] += list.get(i);
                s += list.get(i);
            }
            assertEquals(rowSum, s);
        }
        for (int c = 0; c < listDim; c++) {
            assertEquals(colSum, colAcc[c], nSample/100.0);
        }
    }
}
