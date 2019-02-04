package paris.benoit.mob.loopback;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import co.paralleluniverse.actors.ActorRegistry;
import paris.benoit.mob.message.ClusterMessage;
import co.paralleluniverse.actors.ActorRef;

@SuppressWarnings("serial")
public class ActorLoopBackSink extends RichSinkFunction<Row> {
    
    public static ArrayBlockingQueue<Integer> registerQueue = new ArrayBlockingQueue<Integer>(50);

    JsonRowSerializationSchema jrs = null;
    
    public ActorLoopBackSink(TypeInformation<Row> jsonTypeInfo) {
        this.jrs = new JsonRowSerializationSchema(jsonTypeInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        registerQueue.put(getRuntimeContext().getIndexOfThisSubtask());
    }
    
    @Override
    public void invoke(Row row) throws Exception {
        // call to send: not blocking or dropping the message, as his mailbox is unbounded
//        ((ActorRef<ClusterMessage>)ActorRegistry.getActor(value.f1.loopbackAdress)).send(value.f1);

        System.out.println("new msg in sink: " + new String(jrs.serialize((Row) row.getField(0))));
    }

}
