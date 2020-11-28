package paris.benoit.mob.cluster;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.io.KafkaGlobals;
import paris.benoit.mob.cluster.io.KafkaSchemaRegistry;
import paris.benoit.mob.cluster.utils.TableSchemaConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;
import static paris.benoit.mob.cluster.utils.Colors.yellow;

public class MobTableEnvironment {
    private static final Logger logger = LoggerFactory.getLogger(MobTableEnvironment.class);

    protected static final Pattern RETRACT_PATTERN = Pattern.compile(
            "CREATE RETRACT VIEW (?<name>[^ ]+) AS\\s+(?<query>.*)",
            Pattern.DOTALL);
    protected static final Pattern TEMPORAL_PATTERN = Pattern.compile(
            "CREATE TEMPORAL TABLE FUNCTION (?<name>[^ ]+) TIME ATTRIBUTE (?<time>[^ ]+) PRIMARY KEY (?<key>[^ ]+) AS\\s+(?<query>.*)",
            Pattern.DOTALL);
    protected static final Pattern DEFAULT_PATTERN = Pattern.compile(
            "(?<sql>.*)",
            Pattern.DOTALL);

    public static void sqlUpdate(StreamTableEnvironment tEnv, Catalog catalog, String content) throws TableAlreadyExistException, DatabaseNotExistException {

        Matcher mRetract = RETRACT_PATTERN.matcher(content);
        Matcher mTemporal = TEMPORAL_PATTERN.matcher(content);
        Matcher mDefault = DEFAULT_PATTERN.matcher(content);

        if (mRetract.matches()) {

            String sql = mRetract.group("query");
            tryPrintExplain(tEnv, sql);
            Table fromTable = tEnv.sqlQuery(sql);
            DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(fromTable, Row.class);
            Table retractTable = tEnv.fromDataStream(retractStream, $("accumulate_flag"), $("content"));
            tEnv.createTemporaryView(mRetract.group("name"), retractTable);

        } else if (mTemporal.matches()) {

            String sql = mTemporal.group("query");
            tryPrintExplain(tEnv, sql);
            Table historyTable = tEnv.sqlQuery(sql);
            TemporalTableFunction temporalTable = historyTable.createTemporalTableFunction($(mTemporal.group("time")), $(mTemporal.group("key")));
            tEnv.createTemporarySystemFunction(mTemporal.group("name"), temporalTable);

        } else if (mDefault.matches()){

            String sql = mDefault.group("sql");
            List<Operation> parsed;
            try {
                parsed = ((TableEnvironmentImpl) tEnv).getParser().parse(sql);
            } catch (SqlParserException e) {
                logger.error("Failed to parse:\n" + sql);
                throw e;
            }

            if (parsed.size() == 1 && parsed.get(0) instanceof CreateTableOperation) {
                CreateTableOperation itemCasted = (CreateTableOperation) parsed.get(0);
                ObjectIdentifier identifier = itemCasted.getTableIdentifier();
                CatalogTable catalogTable = itemCasted.getCatalogTable();
                TableSchema schema = catalogTable.getSchema();

                String fullName = identifier.getCatalogName() + "." + identifier.toObjectPath().getFullName();

                KafkaSchemaRegistry.registerSchema(
                        fullName,
                        TableSchemaConverter.toJsonSchema(schema)
                );

                Map<String, String> optionsBefore = catalogTable.getOptions();
                KafkaSchemaRegistry.registerMobOptions(fullName, optionsBefore);

                HashMap<String, String> tableProps = new HashMap<>(catalogTable.toProperties());
                optionsBefore.keySet().forEach(tableProps::remove); // keep only schema info

                tableProps.putAll(KafkaGlobals.getTableOptions(fullName, schema.getPrimaryKey().isPresent()));

                catalog.createTable(
                        // TODO enforce que appli name == schema déclaré
                        //   sauf si on dit que c'est un service explicitement
                        //     faudrait faire passer tout ça par une classe dédiée à des security rules
                        //       cf notes
                        itemCasted.getTableIdentifier().toObjectPath(),
                        CatalogTableImpl.fromProperties(tableProps),
                        itemCasted.isIgnoreIfExists());

            } else {
                tryPrintExplain(tEnv, sql);
                tEnv.sqlUpdate(sql);
            }

        } else {
            throw new RuntimeException("Failed to match default sql matcher");
        }


    }

    private static void tryPrintExplain(StreamTableEnvironment tEnv, String sql) {
        try {
            logger.info(yellow(tEnv.explainSql(sql, ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE)));
        } catch (TableException ignored) {}
    }



}
