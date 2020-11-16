package paris.benoit.mob.cluster.services;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.RowRetractStreamTableSink;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TwitterSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private static final Logger logger = LoggerFactory.getLogger(RowRetractStreamTableSink.class);

    Twitter twitter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        twitter = new TwitterFactory().getInstance();
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) {
        if(value.f0) {
            try {
                twitter.updateStatus((String) value.f1.getField(0));
                logger.info("Tweeted: " + value.f1);
            } catch (TwitterException e) {
                logger.error("Twitter update failed", e);
            }
        }
    }
}
