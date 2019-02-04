package paris.benoit.mob.cluster.loopback;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.cluster.RegistryWeaver;
import paris.benoit.mob.cluster.json2sql.JsonTableSource;
import paris.benoit.mob.cluster.json2sql.NumberedReceivePort;

@SuppressWarnings("serial")
public class ActorSource extends RichParallelSourceFunction<Row> {
    
    private volatile boolean isRunning = true;

    private ThreadReceivePort<Row> receivePort = null;
    private Integer loopbackIndex = null;
    
    public ActorSource(JsonTableSource parentTableParam) {
        super();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        NumberedReceivePort<Row> nrp = RegistryWeaver.registerSource(this);
        receivePort = nrp.getReceiveport();
        loopbackIndex = nrp.getIndex();
    }
    
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !receivePort.isClosed()) {
            // en plus des static Channel et ThreadReceivePort, ici on peut pas passer en generics, sauf à passer un truc qui implement
            //   une interface du style Addressable avec un void setIndex(Integer)
            //     et pourquoi pas avoir l'identité dans le Addressable?
            Row row = receivePort.receive();
            // par convention
            row.setField(0, "" + loopbackIndex);
            sc.collect(row);
        }
    }
    
    public void cancel() {
        isRunning = false;
    }
    
}
