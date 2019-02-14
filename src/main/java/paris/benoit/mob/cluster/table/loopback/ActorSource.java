package paris.benoit.mob.cluster.table.loopback;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.cluster.MobClusterRegistry;
import paris.benoit.mob.cluster.MobClusterSender;

@SuppressWarnings("serial")
public class ActorSource extends RichParallelSourceFunction<Row> {
    
    private static final Logger logger = LoggerFactory.getLogger(ActorSource.class);
    
    private volatile boolean isRunning = true;

    private ThreadReceivePort<Row> receivePort = null;
    private Integer loopbackIndex = -1;
    private JsonRowDeserializationSchema jrds;
    
    public ActorSource(JsonRowDeserializationSchema jrds) {
        super();
        this.jrds = jrds;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MobClusterSender sender = new MobClusterSender(jrds);
        MobClusterRegistry.registerClusterSender(sender);
        receivePort = sender.getReceiveport();
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        logger.info("Opening source #" + loopbackIndex);
    }
    
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !receivePort.isClosed()) {
            // ici on peut pas passer en generics, sauf à passer un truc qui implement
            //   une interface du style Addressable avec un void setIndex(Integer)
            //     et pourquoi pas avoir l'identité dans le Addressable?
            Row row = receivePort.receive();
            // par convention
            row.setField(0, loopbackIndex);
            sc.collect(row);
        }
    }
    
    public void cancel() {
        isRunning = false;
    }

}
