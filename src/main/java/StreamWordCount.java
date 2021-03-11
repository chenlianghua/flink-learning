import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.WordCntFlatter;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<String> inputDataStream = env.socketTextStream("172.16.1.185", 7777);

        // 基于数据流进行转换计算
        // 使用nc -lk 7777开启一个socket通信端口
        DataStream<Tuple2<String,Integer>> resultStream = inputDataStream.flatMap(new WordCntFlatter())
                .keyBy(item->item.f0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }
}
