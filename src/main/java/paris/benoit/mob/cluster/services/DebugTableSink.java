package paris.benoit.mob.cluster.services;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.RowRetractStreamTableSink;

public class DebugTableSink extends RowRetractStreamTableSink {
    private static final Logger logger = LoggerFactory.getLogger(DebugTableSink.class);

    public DebugTableSink() {
        fieldNames = new String[] { "debug_info" };
        fieldTypes = new DataType[] { DataTypes.STRING() };
        sinkFunction = new RichSinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) {
                logger.info(value.toString());
            }
        };
        name = "Debug DataStreamSink";
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        return ds
                .addSink(sinkFunction)
                .name(name);
    }

}