package com.alibaba.ververica.cdc.connectors.mongodb;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.connector.mongodb.MongoDbConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class MongodbSource {
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder class of {@link MongodbSource}.
     */
    public static class Builder<T> {

        private String mongodbName;
        private String mongodbHosts;
        private String mongodbUser;
        private String mongodbPassword;
        private String name;

        private String[] databaseList;
        private String serverTimeZone;
        private String[] tableList;
        private Properties dbzProperties;
        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> mongodbName(String mongodbName) {
            this.mongodbName = mongodbName;
            return this;
        }

        public Builder<T> mongodbHosts(String mongodbHosts) {
            this.mongodbHosts = mongodbHosts;
            return this;
        }

        public Builder<T> mongodbUser(String mongodbUser) {
            this.mongodbUser = mongodbUser;
            return this;
        }

        public Builder<T> mongodbPassword(String mongodbPassword) {
            this.mongodbPassword = mongodbPassword;
            return this;
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }



        /**
         * An optional list of regular expressions that match database names to be monitored;
         * any database name not included in the whitelist will be excluded from monitoring.
         * By default all databases will be monitored.
         */
        public Builder<T> databaseList(String... databaseList) {
            this.databaseList = databaseList;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers
         * for tables to be monitored; any table not included in the list will be excluded from
         * monitoring. Each identifier is of the form databaseName.tableName.
         * By default the connector will monitor every non-system table in each monitored database.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }


        /**
         * The session time zone in database server, e.g. "America/Los_Angeles".
         * It controls how the TIMESTAMP type in MYSQL converted to STRING.
         * See more https://debezium.io/documentation/reference/1.2/connectors/mysql.html#_temporal_values
         */
        public Builder<T> serverTimeZone(String timeZone) {
            this.serverTimeZone = timeZone;
            return this;
        }


        /**
         * The Debezium MySQL connector properties. For example, "snapshot.mode".
         */
        public Builder<T> debeziumProperties(Properties properties) {
            this.dbzProperties = properties;
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", MongoDbConnector.class.getCanonicalName());
            // hard code server name, because we don't need to distinguish it, docs:
            // Logical name that identifies and provides a namespace for the particular MySQL database
            // server/cluster being monitored. The logical name should be unique across all other connectors,
            // since it is used as a prefix for all Kafka topic names emanating from this connector.
            // Only alphanumeric characters and underscores should be used.
            props.setProperty("mongodb.name", checkNotNull(mongodbName));
            props.setProperty("mongodb.hosts", checkNotNull(mongodbHosts));
            props.setProperty("mongodb.user", checkNotNull(mongodbUser));
            props.setProperty("mongodb.password", checkNotNull(mongodbPassword));
            props.setProperty("name", checkNotNull(name));
            props.setProperty("database.server.name", "Mongodb_oplog_source");

            if (databaseList != null) {
                props.setProperty("database.whitelist", String.join(",", databaseList));
            }
            if (tableList != null) {
                props.setProperty("table.whitelist", String.join(",", tableList));
            }
            if (serverTimeZone != null) {
                props.setProperty("database.serverTimezone", serverTimeZone);
            }

            if (dbzProperties != null) {
                dbzProperties.forEach(props::put);
            }

            return new DebeziumSourceFunction<>(
                    deserializer,
                    props);
        }
    }
}

