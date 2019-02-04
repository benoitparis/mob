package paris.benoit.mob.loopback;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import co.paralleluniverse.actors.ActorRegistry;
import co.paralleluniverse.actors.ActorRef;

import paris.benoit.mob.message.OutputRow;

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
        
        Row root = (Row) row.getField(0);
        // par convention? faudrait faire par nom?
        String identity = (String) root.getField(1);
        // call to send: not blocking or dropping the message, as his mailbox is unbounded
        ((ActorRef<OutputRow>)ActorRegistry.getActor(identity)).send(new OutputRow(row.toString()));

        System.out.println("new msg in sink: " + row);
    }

}
