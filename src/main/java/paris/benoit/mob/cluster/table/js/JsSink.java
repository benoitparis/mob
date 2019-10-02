package paris.benoit.mob.cluster.table.js;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.MobTableConfiguration;

public class JsSink extends RichSinkFunction<Tuple2<Boolean, Row>> {

    private MobTableConfiguration configuration;

    public JsSink(MobTableConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsTable.registerSink(configuration.name,this);
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {

    }
}
