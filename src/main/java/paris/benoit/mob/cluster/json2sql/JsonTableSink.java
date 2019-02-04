package paris.benoit.mob.cluster.json2sql;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import paris.benoit.mob.cluster.loopback.ActorSink;

public class JsonTableSink implements AppendStreamTableSink<Row> {

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    private TypeInformation<Row> jsonTypeInfo;

    public JsonTableSink(String schema) {
        jsonTypeInfo = JsonRowSchemaConverter.convert(schema);
        fieldNames = new String[] { 
            "loopback_index", 
            "actor_identity",
            "payload" 
        };
        fieldTypes = new TypeInformation[] {
            Types.STRING(),
            Types.STRING(),
            jsonTypeInfo
        };

        System.out.println(jsonTypeInfo);
        System.out.println(Arrays.asList(fieldTypes));
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new UnsupportedOperationException("This class is configured through its constructor");
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return Types.ROW(fieldNames, fieldTypes);
    }

    @Override
    public void emitDataStream(DataStream<Row> ds) {
        ds.addSink(new ActorSink(jsonTypeInfo));
    }

}
