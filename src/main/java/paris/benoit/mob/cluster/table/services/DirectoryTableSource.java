package paris.benoit.mob.cluster.table.services;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.MobAppConfiguration;

import java.util.List;
import java.util.stream.Collectors;

public class DirectoryTableSource implements StreamTableSource<Row> {

    private static final String[] fieldNames = new String[] { "app_name" };
    private DataType[] fieldTypes = new DataType[] { DataTypes.STRING() };

    private List<MobAppConfiguration> apps;

    public DirectoryTableSource(List<MobAppConfiguration> apps) {
        this.apps = apps;
    }

    @Override
    public String explainSource() {
        return "App Directory";
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
        return sEnv.fromCollection(apps.stream().map(it -> {
            Row row = new Row(1);
            row.setField(0, it.name);
            return row;
        }).collect(Collectors.toList()));
    }

}
