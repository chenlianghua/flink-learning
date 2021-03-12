package source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MySource implements SourceFunction<String> {

    private boolean running = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(new Random().nextGaussian() + "");

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
