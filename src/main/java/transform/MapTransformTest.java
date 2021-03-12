package transform;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * 输出字符串的长度
 */
public class MapTransformTest implements MapFunction<String, Integer> {

    @Override
    public Integer map(String s) throws Exception {
        return s.length();
    }
}
