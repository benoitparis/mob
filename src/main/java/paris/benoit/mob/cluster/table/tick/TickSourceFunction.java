package paris.benoit.mob.cluster.table.tick;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class TickSourceFunction extends RichParallelSourceFunction<Row> implements ListCheckpointed<Long>  {
    private static final Logger logger = LoggerFactory.getLogger(TickSourceFunction.class);

    private volatile boolean isRunning = true;

    private final long interval;
    private final long offset;
    private final long initialTs;

    private Long counter;


    public TickSourceFunction(long interval) {
        this(0L, interval);
    }

    public TickSourceFunction(long offset, long interval) {
        this.interval = interval;
        this.offset = offset;
        this.initialTs = System.currentTimeMillis();
        this.counter = 0L;
    }

    public void run(SourceContext<Row> sc) throws Exception {
        final Object lock = sc.getCheckpointLock();

        while (isRunning) {

            Row row = new Row(3);
            row.setField(0, (offset + counter));
            row.setField(1, "1");
            row.setField(2, null);

            Thread.sleep(Math.max(0, (initialTs + interval * counter) - System.currentTimeMillis()));

            synchronized (lock) {
                sc.collect(row);
                counter ++;
            }

        }

    }

    public void cancel() {
        isRunning = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) {
        return Collections.singletonList(counter);
    }

    @Override
    public void restoreState(List<Long> state) {
        for (Long s : state) {
            counter = s;
        }
    }

}
