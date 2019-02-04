package paris.benoit.mob.json2sql;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class JsonTableSink implements AppendStreamTableSink<Row> {

    String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    TypeInformation<Row> jsonTypeInfo;

    public JsonTableSink(String schema) {
        jsonTypeInfo = JsonRowSchemaConverter.convert(schema);
        fieldNames = new String[] { "payloadOut" };
        fieldTypes = new TypeInformation[] { jsonTypeInfo };

        System.out.println(jsonTypeInfo);
        System.out.println(Arrays.asList(fieldTypes));
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public String[] getFieldNames() {
        return new String[] { "payloadOut" };
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
        ds.addSink(new RichSinkFunction<Row>() {

            JsonRowSerializationSchema jrs = new JsonRowSerializationSchema(jsonTypeInfo);

            @Override
            public void invoke(Row row, Context context) throws Exception {

                System.out.println(new String(jrs.serialize((Row) row.getField(0))));

            }

        });

    }

}
