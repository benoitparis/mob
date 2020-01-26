package paris.benoit.mob.cluster.table.loopback;

import co.paralleluniverse.strands.channels.ThreadReceivePort;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterRegistry;
import paris.benoit.mob.cluster.MobClusterSender;
import paris.benoit.mob.cluster.MobTableConfiguration;

@SuppressWarnings("serial")
public class ActorSource extends RichParallelSourceFunction<Row> {
    
    private static final Logger logger = LoggerFactory.getLogger(ActorSource.class);
    
    private volatile boolean isRunning = true;

    private ThreadReceivePort<Row> receivePort = null;
    private Integer loopbackIndex = -1;
    private JsonRowDeserializationSchema jrds;
    private MobTableConfiguration configuration;
    
    public ActorSource(MobTableConfiguration configuration, JsonRowDeserializationSchema jrds) {
        super();
        this.jrds = jrds;
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MobClusterSender sender = new MobClusterSender(jrds);
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        MobClusterRegistry.registerClusterSender(configuration.name, sender, loopbackIndex);
        receivePort = sender.getReceiveport();
        logger.info("Opening source #" + loopbackIndex + " (" + configuration.name + ")");
    }
    
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !receivePort.isClosed()) {
            Row row = receivePort.receive();
            // By convention
            row.setField(0, loopbackIndex);
            row.setField(3, "1"); // temporary, to be removed when Blink can to Tables and not TableSources
            sc.collect(row);

        }
    }
    
    public void cancel() {
        isRunning = false;
    }

}
