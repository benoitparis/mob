package paris.benoit.mob.cluster.table.services;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TickTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

    private static final Logger logger = LoggerFactory.getLogger(TickTableSource.class);

    private static final String[] fieldNames = new String[] {
            "tick_number",
            "constant_dummy_source", //TODO remove?
            "proctime_append_stream"
    };
    private DataType[] fieldTypes = new DataType[] {
            DataTypes.BIGINT(),
            DataTypes.STRING(),
            DataTypes.TIMESTAMP(3)
    };
    private long offset = 0, interval;

    public TickTableSource(long offset, long interval) {
        this.offset = offset;
        this.interval = interval;
    }
    public TickTableSource(long interval) {
        this.interval = interval;
    }

    @Override
    public String explainSource() {
        return "Tick Source";
    }

    @Override
    public DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    // TODO faudra virer ça quand ils seront prêt pour les types
    @Override
    public TypeInformation<Row> getReturnType() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());

    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv
            .addSource(new TickSourceFunction(offset, interval), getReturnType())
            .forceNonParallel()
            .name("Tick Source (" + interval + " ms)");
    }

    @Override
    public String getProctimeAttribute() {
        return "proctime_append_stream";
    }
}
