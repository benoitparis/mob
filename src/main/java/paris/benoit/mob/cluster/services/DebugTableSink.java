package paris.benoit.mob.cluster.services;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import paris.benoit.mob.cluster.TypedRetractStreamTableSink;

public class DebugTableSink extends TypedRetractStreamTableSink<String> {

    public DebugTableSink() {
        fieldNames = new String[] { "debug_info" };
        fieldTypes = new DataType[] { DataTypes.STRING() };
        sinkFunction = new RichSinkFunction<Tuple2<Boolean, String>>() {
            @Override
            public void invoke(Tuple2<Boolean, String> value, Context context) {
                System.out.println(value);
            }
        };
        name = "Debug DataStreamSink";
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, String>> ds) {
        return ds
                .addSink(sinkFunction)
                .name(name);
    }

}