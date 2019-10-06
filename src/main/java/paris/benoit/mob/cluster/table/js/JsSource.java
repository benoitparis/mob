package paris.benoit.mob.cluster.table.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
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

    private JsonRowDeserializationSchema jrds;
    private ObjectMapper mapper = new ObjectMapper();

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
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        // TODO fix seri/deseri
        Map item = queue.take();
        String asString = mapper.writeValueAsString(item);

        logger.debug("ppppppppppppppppppppppppppppppppppppppppppppppppppp");
        logger.debug(item.toString());
        logger.debug(asString);

        Row row = new Row(1);
        // By convention
        row.setField(0, jrds.deserialize(asString.getBytes()));
        ctx.collect(row);
    }

    @Override
    public void cancel() {}

}
