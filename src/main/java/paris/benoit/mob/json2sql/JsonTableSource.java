package paris.benoit.mob.json2sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import paris.benoit.mob.loopback.JsonActorEntrySource;

public class JsonTableSource implements StreamTableSource<Row> {
    
    TypeInformation<Row> jsonTypeInfo;
    
    public JsonTableSource(String schema) {
        jsonTypeInfo = JsonRowSchemaConverter.convert(schema);
    }

    @Override
    public String explainSource() {
        return "Json source";
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW(new String[] {"payload"}, new TypeInformation[] {jsonTypeInfo});
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
            .field("payload", jsonTypeInfo)
            .build();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new JsonActorEntrySource(jsonTypeInfo), getReturnType());
    }

}
