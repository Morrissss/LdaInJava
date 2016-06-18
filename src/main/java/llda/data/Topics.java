package llda.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Topics {

    private List<String> labels;
    private Map<String, Integer> labelIdx;

    public Topics() {
        this.labels = new ArrayList<>();
        this.labelIdx = new HashMap<>();
    }

    public int size() {
        return labels.size();
    }

    public int idx(String label) {
        return labelIdx.get(label);
    }

    public String label(int idx) {
        return labels.get(idx);
    }

    public int addLabel(String label) {
        Integer idx = labelIdx.get(label);
        if (idx == null) {
            idx = labels.size();
            labels.add(label);
            labelIdx.put(label, idx);
        }
        return idx;
    }
}
