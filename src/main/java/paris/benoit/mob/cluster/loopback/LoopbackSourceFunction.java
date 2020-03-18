package paris.benoit.mob.cluster.loopback;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.message.ToServerMessage;

@SuppressWarnings("serial")
class LoopbackSourceFunction extends RichParallelSourceFunction<Row> {
    private static final Logger logger = LoggerFactory.getLogger(LoopbackSourceFunction.class);

    private Integer loopbackIndex = -1;
    private final JsonRowDeserializationSchema jrds;
    private final MobTableConfiguration configuration;
    private ClusterSender sender;

    private volatile boolean isRunning = true;
    
    public LoopbackSourceFunction(MobTableConfiguration configuration, DataType jsonDataType) {
        super();
        this.jrds = new JsonRowDeserializationSchema.Builder((TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(jsonDataType)).build();
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sender = new ClusterSender();
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        ClusterRegistry.registerClusterSender(configuration.fullyQualifiedName(), sender, loopbackIndex);
        logger.info("Opening source #" + loopbackIndex + " (" + configuration.fullyQualifiedName() + ")");
    }

    @Override
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !sender.isClosed()) {

            ToServerMessage msg = sender.receive();

            Row root = new Row(LoopbackTableSource.FIELD_COUNT);
            root.setField(0, loopbackIndex);
            root.setField(1, msg.from);
            root.setField(2, jrds.deserialize(msg.payload.toString().getBytes()));
            root.setField(3, "1"); // temporary, to be removed when Blink can to Tables and not TableSources
            root.setField(4, System.currentTimeMillis());
            sc.collect(root);

        }
    }
    
    public void cancel() {
        isRunning = false;
    }

}
