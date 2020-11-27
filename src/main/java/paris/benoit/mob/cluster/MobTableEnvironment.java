package paris.benoit.mob.cluster;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.distributed.KafkaSchemaRegistry;
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

            Parser parser = ((TableEnvironmentImpl) tEnv).getParser();

            List<Operation> parsed;
            try {
                parsed = parser.parse(sql);
            } catch (SqlParserException e) {
                logger.error("Failed to parse:\n" + sql);
                throw e;
            }


            if (parsed.size() == 1 && parsed.get(0) instanceof CreateTableOperation) {
                CreateTableOperation itemCasted = (CreateTableOperation) parsed.get(0);
                ObjectIdentifier identifier = itemCasted.getTableIdentifier();
                CatalogTable catalogTable = itemCasted.getCatalogTable();

                // TODO changer les props ici (nom, conf generation, registration, etc.)

                String fullName = identifier.getCatalogName() + "." + identifier.toObjectPath().getFullName();

                KafkaSchemaRegistry.registerSchema(
                        fullName,
                        TableSchemaConverter.toJsonSchema(catalogTable.getSchema())
                );

                Map<String, String> optionsBefore = catalogTable.getOptions();
                KafkaSchemaRegistry.registerMobOptions(fullName, optionsBefore);

                HashMap<String, String> tableProps = new HashMap<>(catalogTable.toProperties());
                optionsBefore.keySet().forEach(tableProps::remove); // keep only schema info

                HashMap<String, String> optionsAfter = new HashMap<>();
                optionsAfter.putAll(KAFKA_TABLE_OPTIONS);
                // TODO have constant
                optionsAfter.put("connector.topic", fullName);

                tableProps.putAll(optionsAfter);

                // TODO Finished? No:
                //   1. DONE Remove MobTableConf Properties
                //   2. DONE use Options in KafkaSchemaRegistry, remove properties
                //   3. Modify sql table files
                //   4. DONE Think about services vs js-engine/client: defined globally? defined locally? prob both?
                //       globally: global_tick, inter_app_messaging, debug, directory (+rewrite with global registry)
                //       locally: local_tick, twitter,
                //    TODO redo this: on a confondu déclaration locale du service vs kafka or not vs
                //       -> anyway, app conf should not declare services tables, only official services should
                //      5. Think about defaulting to client
                //   6. TODO quite à utiliser/parasiter les props des tables, autant faire un connector officiel
                //        mob-client, mob-js-engine, ?mob-service (pour un appel à un service global/local?)
                //          par contre faut wrap du kafka? ça fait un peu peur
                //            on procédera par tests, et tout rentrera dans l'ordre?
                //              en tout cas on persévère actuellement dans le cleanup
                //   7. TODO revoir ces copies de map ici

                CatalogTableImpl newTable = CatalogTableImpl.fromProperties(tableProps);

                catalog.createTable(
                        itemCasted.getTableIdentifier().toObjectPath(),
                        newTable,
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

    protected static final Map<String, String> KAFKA_TABLE_OPTIONS = new HashMap<>();
    static {
        // TODO get reference options
        KAFKA_TABLE_OPTIONS.put("connector.type", "kafka");
        KAFKA_TABLE_OPTIONS.put("connector.version", "universal");
        KAFKA_TABLE_OPTIONS.put("connector.property-version", "1");
        KAFKA_TABLE_OPTIONS.put("connector.properties.bootstrap.servers", "localhost:9092");
        KAFKA_TABLE_OPTIONS.put("connector.properties.zookeeper.connect", "localhost:2181");
        KAFKA_TABLE_OPTIONS.put("format.type", "json");
    }

}
