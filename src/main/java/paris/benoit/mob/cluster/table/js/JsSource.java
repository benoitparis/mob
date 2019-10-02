package paris.benoit.mob.cluster.table.js;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.MobTableConfiguration;

public class JsSource extends RichParallelSourceFunction<Row> {


    private MobTableConfiguration configuration;

    public JsSource(MobTableConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsTable.registerSource(configuration.name, this);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {

    }

    @Override
    public void cancel() {}


}
