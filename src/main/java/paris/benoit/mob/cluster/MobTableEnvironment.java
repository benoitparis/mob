package paris.benoit.mob.cluster;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;

public class MobTableEnvironment {

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

        Parser parser = ((TableEnvironmentImpl) tEnv).getParser();

        Matcher mRetract = RETRACT_PATTERN.matcher(content);
        Matcher mTemporal = TEMPORAL_PATTERN.matcher(content);
        Matcher mDefault = DEFAULT_PATTERN.matcher(content);

        if (mRetract.matches()) {

            Table fromTable = tEnv.sqlQuery(mRetract.group("query"));
            DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(fromTable, Row.class);
            Table retractTable = tEnv.fromDataStream(retractStream, $("accumulate_flag"), $("content"));
            tEnv.createTemporaryView(mRetract.group("name"), retractTable);

        } else if (mTemporal.matches()) {

            Table historyTable = tEnv.sqlQuery(mTemporal.group("query"));
            TemporalTableFunction temporalTable = historyTable.createTemporalTableFunction($(mTemporal.group("time")), $(mTemporal.group("key")));
            tEnv.registerFunction(mTemporal.group("name"), temporalTable);

        } else if (mDefault.matches()){

            String sql = mDefault.group("sql");

            List<Operation> parsed = parser.parse(sql);
            if (parsed.size() == 1 && parsed.get(0) instanceof CreateTableOperation) {
                CreateTableOperation itemCasted = (CreateTableOperation) parsed.get(0);
                CatalogTable catalogTable = itemCasted.getCatalogTable();

                // TODO changer les props ici (nom, conf generation, registration, etc.)
                //   ie nettoyer MobTableConfiguration.buildProperties
                CatalogTableImpl newTable = CatalogTableImpl.fromProperties(catalogTable.toProperties());

                catalog.createTable(
                        itemCasted.getTableIdentifier().toObjectPath(),
                        newTable,
                        itemCasted.isIgnoreIfExists());

            } else {
                tEnv.sqlUpdate(sql);
            }

        } else {
            throw new RuntimeException("Failed to match default sql matcher");
        }


    }
}
