package paris.benoit.mob.cluster.table.js;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.concurrent.BlockingQueue;

public class JsSource extends RichParallelSourceFunction<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsSource.class);

    private MobTableConfiguration configuration;
    private BlockingQueue<Object> queue;

    public JsSource(MobTableConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("Parallelism of jsEngine source " + configuration.name + " : " + getRuntimeContext().getNumberOfParallelSubtasks());
        queue = JsTableEngine.registerSource(configuration.name);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        ctx.collect((Row) queue.take());
    }

    @Override
    public void cancel() {}

}
