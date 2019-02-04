package paris.benoit.mob.cluster.json2sql;

import co.paralleluniverse.strands.channels.ThreadReceivePort;

public class NumberedReceivePort<T> {
    
    Integer index = -1;
    ThreadReceivePort<T> receiveport;

    public NumberedReceivePort(ThreadReceivePort<T> receiveport, Integer index) {
        super();
        this.index = index;
        this.receiveport = receiveport;
    }

    public Integer getIndex() {
        return index;
    }

    public ThreadReceivePort<T> getReceiveport() {
        return receiveport;
    }
    
}
