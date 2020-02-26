package paris.benoit.mob.cluster.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

class JsSourceFunction extends RichParallelSourceFunction<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsSourceFunction.class);

    private final MobTableConfiguration parentConfiguration;
    private BlockingQueue<Map> queue;
    private CompletableFuture<BlockingQueue<Map>> future;
    private volatile boolean isRunning = true;

    private final JsonRowDeserializationSchema jrds;
    private ObjectMapper mapper;

    public JsSourceFunction(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration) {
        this.parentConfiguration = parentConfiguration;

        TypeInformation<Row> jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        jrds = new JsonRowDeserializationSchema.Builder(jsonTypeInfo).build();
        logger.info("Instanciated JsSourceFunction with json schema: " + jsonTypeInfo.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("Parallelism of jsEngine source " + parentConfiguration.name + " : " + getRuntimeContext().getNumberOfParallelSubtasks());
        future = JsTableEngine.registerSource(parentConfiguration.name);
        mapper = new ObjectMapper();
        logger.info("Opened JsSourceFunction");
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        queue = future.get();

        while (isRunning) {
            Map item = queue.take();

            // By convention
            Row row = new Row(2);
            // TODO fix seri/deseri
            row.setField(0, jrds.deserialize(mapper.writeValueAsString(item).getBytes()));
            //logger.debug("" + row);
            ctx.collect(row);
        }

    }

    @Override
    public void cancel() { isRunning = false; }

}
