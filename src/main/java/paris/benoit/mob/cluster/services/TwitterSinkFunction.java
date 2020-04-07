package paris.benoit.mob.cluster.services;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.TypedRetractStreamTableSink;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;

public class TwitterSinkFunction extends RichSinkFunction<Tuple2<Boolean, String>> {
    private static final Logger logger = LoggerFactory.getLogger(TypedRetractStreamTableSink.class);

    Twitter twitter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        twitter = new TwitterFactory().getInstance();
    }

    @Override
    public void invoke(Tuple2<Boolean, String> value, Context context) throws Exception {
        if(value.f0) {
            twitter.updateStatus(value.f1);
            logger.info("Tweeted: " + value.f1);
        }
    }
}
