package paris.benoit.mob.cluster.table.js;

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

public class JsSource extends RichParallelSourceFunction<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsSource.class);

    private MobTableConfiguration parentConfiguration;
    private BlockingQueue<Map> queue;
    private volatile boolean isRunning = true;

    private JsonRowDeserializationSchema jrds;
    private ObjectMapper mapper;

    public JsSource(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration) {
        this.parentConfiguration = parentConfiguration;

        TypeInformation<Row> jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        jrds = new JsonRowDeserializationSchema.Builder(jsonTypeInfo).build();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("Parallelism of jsEngine source " + parentConfiguration.name + " : " + getRuntimeContext().getNumberOfParallelSubtasks());
        queue = JsTableEngine.registerSource(parentConfiguration.name);
        mapper = new ObjectMapper();
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {

        while (isRunning) {
            Map item = queue.take();

            // By convention
            Row row = new Row(1);
            // TODO fix seri/deseri
            row.setField(0, jrds.deserialize(mapper.writeValueAsString(item).getBytes()));
            logger.debug("" + row);
            ctx.collect(row);
        }

    }

    @Override
    public void cancel() { isRunning = false; }

}
