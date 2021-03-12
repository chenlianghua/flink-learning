package transform;

import org.apache.flink.api.common.functions.FilterFunction;

public class FilterTransformTest implements FilterFunction<String> {
    @Override
    public boolean filter(String s) throws Exception {
        return s.startsWith("flink");
    }
}
