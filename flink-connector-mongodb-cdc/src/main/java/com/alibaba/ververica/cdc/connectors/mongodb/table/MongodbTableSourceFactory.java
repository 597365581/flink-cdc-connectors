package com.alibaba.ververica.cdc.connectors.mongodb.table;

import com.alibaba.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

public class MongodbTableSourceFactory implements DynamicTableSourceFactory {
    private static final String IDENTIFIER = "mongodb-cdc";

    private static final ConfigOption<String> MONGODBNAME = ConfigOptions.key("mongodbName")
            .stringType()
            .noDefaultValue()
            .withDescription("A unique name that identifies the connector and/or MongoDB replica set or sharded cluster that this connector monitors. Each server should be monitored by at most one Debezium connector, since this server name prefixes all persisted Kafka topics emanating from the MongoDB replica set or cluster. Only alphanumeric characters and underscores should be used.");

    private static final ConfigOption<String> MONGODBHOSTS = ConfigOptions.key("mongodbHosts")
            .stringType()
            .noDefaultValue()
            .withDescription("The comma-separated list of hostname and port pairs (in the form 'host' or 'host:port') of the MongoDB servers in the replica set. The list can contain a single hostname and port pair. If mongodb.members.auto.discover is set to false, then the host and port pair should be prefixed with the replica set name (e.g., rs0/localhost:27017).");

    private static final ConfigOption<String> NAME = ConfigOptions.key("name")
            .stringType()
            .noDefaultValue()
            .withDescription("A unique name that identifies the connector and/or MongoDB replica set or sharded cluster that this connector monitors. Each server should be monitored by at most one Debezium connector, since this server name prefixes all persisted Kafka topics emanating from the MongoDB replica set or cluster. Only alphanumeric characters and underscores should be used.");



    private static final ConfigOption<String> MONGODBUSER = ConfigOptions.key("mongodbUser")
            .stringType()
            .noDefaultValue()
            .withDescription("Name of the database user to be used when connecting to MongoDB. This is required only when MongoDB is configured to use authentication.");

    private static final ConfigOption<String> MONGODBPASSWORD = ConfigOptions.key("mongodbPassword")
            .stringType()
            .noDefaultValue()
            .withDescription("Password to be used when connecting to MongoDB. This is required only when MongoDB is configured to use authentication.");

    private static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
            .stringType()
            .noDefaultValue()
            .withDescription("Database name of the Mongodb server to monitor.");

    private static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("Table name of the Mongodb database to monitor.");

    private static final ConfigOption<String> SERVER_TIME_ZONE = ConfigOptions.key("server-time-zone")
            .stringType()
            .defaultValue("UTC")
            .withDescription("The session time zone in database server.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String mongodbHosts = config.get(MONGODBHOSTS);
        String mongodbUser = config.get(MONGODBUSER);
        String mongodbPassword = config.get(MONGODBPASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String mongodbName = config.get(MONGODBNAME);
        String name = config.get(NAME);
        ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new MongodbTableSource(
                physicalSchema,
                mongodbName,
                mongodbHosts,
                databaseName,
                tableName,
                mongodbUser,
                mongodbPassword,
                name,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions())
        );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MONGODBHOSTS);
        options.add(MONGODBUSER);
        options.add(MONGODBPASSWORD);
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(MONGODBNAME);
        options.add(NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SERVER_TIME_ZONE);
        return options;
    }
}
