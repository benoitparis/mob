package paris.benoit.mob.cluster.loopback;

import co.paralleluniverse.strands.channels.ThreadReceivePort;
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

@SuppressWarnings("serial")
class ActorSource extends RichParallelSourceFunction<Row> {
    
    private static final Logger logger = LoggerFactory.getLogger(ActorSource.class);
    
    private volatile boolean isRunning = true;

    private ThreadReceivePort<Row> receivePort = null;
    private Integer loopbackIndex = -1;
    private final JsonRowDeserializationSchema jrds;
    private final MobTableConfiguration configuration;
    
    public ActorSource(MobTableConfiguration configuration, DataType jsonDataType) {
        super();
        this.jrds = new JsonRowDeserializationSchema.Builder((TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(jsonDataType)).build();
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ClusterSender sender = new ClusterSender(jrds);
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        ClusterRegistry.registerClusterSender(configuration.fullyQualifiedName(), sender, loopbackIndex);
        receivePort = sender.getReceiveport();
        logger.info("Opening source #" + loopbackIndex + " (" + configuration.fullyQualifiedName() + ")");
    }
    
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !receivePort.isClosed()) {
            Row row = receivePort.receive();
            // By convention
            row.setField(0, loopbackIndex);
            row.setField(3, "1"); // temporary, to be removed when Blink can to Tables and not TableSources
            row.setField(4, System.currentTimeMillis());
            sc.collect(row);

        }
    }
    
    public void cancel() {
        isRunning = false;
    }

}
