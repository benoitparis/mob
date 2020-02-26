package paris.benoit.mob.cluster.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

class JsSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private static final Logger logger = LoggerFactory.getLogger(JsSinkFunction.class);

    private final MobTableConfiguration parentConfiguration;
    private BlockingQueue<Map> queue;

    private final JsonRowSerializationSchema jrs;

    private final String invokeFunction;
    private final String code;
    private Invocable inv;

    public JsSinkFunction(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration, String invokeFunction, String code) {
        this.parentConfiguration = parentConfiguration;

        TypeInformation<Row> jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        jrs = JsonRowSerializationSchema.builder().withTypeInfo(jsonTypeInfo).build();

        this.invokeFunction = invokeFunction;
        this.code = code;
        logger.info("Instanciated JsSinkFunction with json schema: " + jsonTypeInfo.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("Parallelism of jsEngine sink " + parentConfiguration.name + " : " + getRuntimeContext().getNumberOfParallelSubtasks());
        queue = JsTableEngine.registerSink(parentConfiguration.name);

        ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
        graaljsEngine.eval(code);
        inv = (Invocable) graaljsEngine;

        logger.info("Opened JsSinkFunction" );
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) {

        if (value.f0) {
            // par convention
            Row payload = (Row) value.f1.getField(0);

            try {
                Map out = (Map) inv.invokeFunction(invokeFunction, convertRowToMap(payload));
                @SuppressWarnings("unchecked") HashMap copy = new HashMap(out); // prevent multithreaded access
                queue.add(copy);
            } catch (NoSuchMethodException | ScriptException e) {
                throw new RuntimeException("Js invocation failed", e);
            }

        }

    }

    private String convertRowToMap(Row row) {
        // TODO fix seri/deseri
//        return mapper.readValue(jrs.serialize(row), type);
        return new String(jrs.serialize(row));
    }

}
