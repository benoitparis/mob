package paris.benoit.mob.cluster.services;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.RowStreamTableSource;

public class TickTableSource extends RowStreamTableSource implements DefinedProctimeAttribute {

    private static final String[] FIELD_NAMES = new String[] {
            "tick_number",
            "constant_dummy_source",
            "proctime_append_stream"
    };
    private final DataType[] FIELD_TYPES = new DataType[] {
            DataTypes.BIGINT(),
            DataTypes.STRING(),
            DataTypes.TIMESTAMP(3)
    };

    public TickTableSource(long offset, long interval) {
        fieldNames = FIELD_NAMES;
        fieldTypes = FIELD_TYPES;
        sourceFunction = new TickSourceFunction(offset, interval);
        name = "Tick Source (" + interval + " ms)";
    }

    public TickTableSource(long interval) {
        this(0, interval);
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv
            .addSource(sourceFunction, getReturnType())
            .forceNonParallel()
            .name(explainSource());
    }

    @Override
    public String getProctimeAttribute() {
        return "proctime_append_stream";
    }
}
