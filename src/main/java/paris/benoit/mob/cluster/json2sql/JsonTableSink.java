package paris.benoit.mob.cluster.json2sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.RegistryWeaver;
import paris.benoit.mob.cluster.loopback.ActorSink;

public class JsonTableSink implements AppendStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSink.class);

    private TypeInformation<Row> jsonTypeInfo;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

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
        logger.info("Created Sink with json schema: ");
        logger.info(jsonTypeInfo.toString());
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
