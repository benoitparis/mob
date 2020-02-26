package paris.benoit.mob.cluster.services;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.MobAppConfiguration;
import paris.benoit.mob.cluster.RowStreamTableSource;

import java.util.List;
import java.util.stream.Collectors;

public class DirectoryTableSource extends RowStreamTableSource implements DefinedProctimeAttribute {

    private static final String[] FIELD_NAMES = new String[] {
            "app_name",
            "constant_dummy_source",
            "proctime_append_stream"
    };
    private final DataType[] FIELD_TYPES = new DataType[] {
            DataTypes.STRING(),
            DataTypes.STRING(),
            DataTypes.TIMESTAMP(3)
    };

    private final List<MobAppConfiguration> apps;

    public DirectoryTableSource(List<MobAppConfiguration> apps) {
        fieldNames = FIELD_NAMES;
        fieldTypes = FIELD_TYPES;
        name = "App Directory";
        this.apps = apps;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv.fromCollection(
                apps.stream().map(it -> {
                        Row row = new Row(3);
                        row.setField(0, it.name);
                        row.setField(1, "1");
                        return row;
                    }).collect(Collectors.toList()),
                    getReturnType()
                )
                .forceNonParallel()
                .name(explainSource());
    }

    @Override
    public String getProctimeAttribute() {
        return "proctime_append_stream";
    }
}
