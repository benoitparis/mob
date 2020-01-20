package paris.benoit.mob.cluster.table.tick;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class TickKeyedProcessFunction extends KeyedProcessFunction<Long, Long, Row> {

    private long interval;
    private ValueState<Long> counter;


    public TickKeyedProcessFunction(long interval) {
        this.interval = interval;
    }

    public TickKeyedProcessFunction(int interval) {
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        counter = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tickCounter", Long.class));
    }

    @Override
    // called once
    public void processElement(Long value, Context context, Collector<Row> collector) throws IOException {
        counter.update(value);
        context.timerService().registerEventTimeTimer(System.currentTimeMillis());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        Long current = counter.value();
        current++;
        counter.update(current);

        Long now = System.currentTimeMillis();
        ctx.timerService().registerEventTimeTimer(now + interval);

        Row result = new Row(1);
        result.setField(0, current);
        out.collect(result);
    }

}
