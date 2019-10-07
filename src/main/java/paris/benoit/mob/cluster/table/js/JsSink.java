package paris.benoit.mob.cluster.table.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class JsSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private static final Logger logger = LoggerFactory.getLogger(JsSink.class);

    private MobTableConfiguration parentConfiguration;
    private BlockingQueue<Map> queue;
    private Consumer<String> consumer;

    private JsonRowSerializationSchema jrs;
    private ObjectMapper mapper = new ObjectMapper();
//    private MapType type = mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);

    String invokeFunction;
    String code;

    JsSink(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration, String invokeFunction, String code) {
        this.parentConfiguration = parentConfiguration;

        TypeInformation<Row> jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        jrs = new JsonRowSerializationSchema.Builder(jsonTypeInfo).build();

        this.invokeFunction = invokeFunction;
        this.code = code;
        logger.info("Created Sink with json schema: " + jsonTypeInfo.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("Parallelism of jsEngine sink " + parentConfiguration.name + " : " + getRuntimeContext().getNumberOfParallelSubtasks());
        queue = JsTableEngine.registerSink(parentConfiguration.name);

        ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
        graaljsEngine.eval(code);
        Invocable inv = (Invocable) graaljsEngine;

        consumer = it -> {
            try {
                queue.add((Map) inv.invokeFunction(invokeFunction, it));
            } catch (NoSuchMethodException | ScriptException e) {
                throw new RuntimeException("Js invocation failed", e);
            }
        };

    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) {

        if (value.f0) {
            Row row = value.f1;
            // par convention
            //Integer insertTime = (Integer) row.getField(0); ?
            Row payload = (Row) row.getField(1);
            consumer.accept(convertRowToMap(payload));
        }

    }

    private String convertRowToMap(Row row) {
        // TODO fix seri/deseri
//        return mapper.readValue(jrs.serialize(row), type);
        return new String(jrs.serialize(row));
    }

}
