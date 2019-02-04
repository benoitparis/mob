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

    @SuppressWarnings("serial")
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        
        return execEnv.addSource(new SourceFunction<Row>() {
            
            JsonRowDeserializationSchema jrds = new JsonRowDeserializationSchema(jsonTypeInfo);

            @Override
            public void cancel() {
            }

            @Override
            public void run(SourceContext<Row> sc) throws Exception {
                Row root = new Row(1);
                Row payload = jrds.deserialize("{ \"col1\" : \"dasdasdas\", \"col2\": { \"colA\":111, \"colB\": 222}} ".getBytes());
                root.setField(0, payload);
                sc.collect(root);
            }
            
        }, getReturnType());
    }

}
