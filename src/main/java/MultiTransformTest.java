import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MultiTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取文本数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\公司代码\\java\\flink-learning\\testData\\sample.txt");

        //split分流的方法已经被弃用了，转而用outputTag方式去分流，output的分流结果能被再次分流，但是split方法不行
        OutputTag<String> flinkTag = new OutputTag<String>("flinkString");
        OutputTag<String> otherTag = new OutputTag<String>("otherStream");

        SingleOutputStreamOperator<Object> processStream = inputStream.process(new ProcessFunction<String, Object>() {

            @Override
            public void processElement(String s, Context context, Collector<Object> collector) throws Exception {
                if (s.startsWith("flink")) {
                    context.output(flinkTag, s);
                } else {
                    context.output(otherTag, s);
                }
            }
        });



        env.execute();

    }
}
