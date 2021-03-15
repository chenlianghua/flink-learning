import org.apache.flink.streaming.api.datastream.DataStream;
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

        //split分流的方法已经被弃用了，转而用outputTag方式去分流
        //注意：OutputTag new 出来的结果必须得是匿名类（构造方法后面必须跟"{}"）
        OutputTag<String> flinkTag = new OutputTag<String>("flinkString") {};
        OutputTag<String> otherTag = new OutputTag<String>("otherStream") {};

        SingleOutputStreamOperator<String> processStream = inputStream.process(new ProcessFunction<String, String>() {

            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                if (s.startsWith("flink")) {
                    context.output(flinkTag, s);
                } else {
                    context.output(otherTag, s);
                }
            }
        });

        DataStream<String> flinkStream = processStream.getSideOutput(flinkTag);
        DataStream<String> otherStream = processStream.getSideOutput(otherTag);

        flinkStream.print("flink stream");
        otherStream.print("other stream");

        env.execute();

    }
}
