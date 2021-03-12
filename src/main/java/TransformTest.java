import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import transform.FilterTransformTest;
import transform.MapTransformTest;

public class TransformTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件读取文本数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\公司代码\\java\\flink-learning\\testData\\sample.txt");

        // map：单值转换
        SingleOutputStreamOperator<Integer> mapStream = inputStream.map(new MapTransformTest());
        mapStream.print("map");

        // flat map：一个输入，多个输出
        SingleOutputStreamOperator<String> flatmapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");

                for (String field: fields) {
                    collector.collect(field);
                }
            }
        });
        flatmapStream.print("flatMap");

        // filter：过滤符合要求的数据
        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterTransformTest());
        filterStream.print("filter");

        env.execute("Transform Test Job");
    }
}
