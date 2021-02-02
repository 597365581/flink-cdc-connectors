package com.alibaba.ververica.cdc.connectors.mongodb.table;

import com.alibaba.ververica.cdc.connectors.mongodb.MongodbSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class MongodbTableSource implements ScanTableSource {
    private final TableSchema physicalSchema;

    private String mongodbName;
    private String mongodbHosts;
    private String mongodbUser;
    private String mongodbPassword;
    private String name;
    private final String database;
    private final String tableName;
    private final ZoneId serverTimeZone;
    private final Properties dbzProperties;

    public MongodbTableSource(
            TableSchema physicalSchema,
            String mongodbName,
            String mongodbHosts,
            String database,
            String tableName,
            String mongodbUser,
            String mongodbPassword,
            String name,
            ZoneId serverTimeZone,
            Properties dbzProperties) {
        this.physicalSchema = physicalSchema;
        this.mongodbName = mongodbName;
        this.mongodbHosts = checkNotNull(mongodbHosts);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.mongodbUser = mongodbUser;
        this.mongodbPassword = mongodbPassword;
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.name = name;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = (TypeInformation<RowData>) scanContext.createTypeInformation(physicalSchema.toRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
                rowType,
                typeInfo,
                ((rowData, rowKind) -> {
                }),
                serverTimeZone);
        MongodbSource.Builder<RowData> builder = MongodbSource.<RowData>builder()
                .mongodbName(mongodbName)
                .mongodbHosts(mongodbHosts)
                .databaseList(database)
                .tableList(database + "." + tableName)
                .mongodbUser(mongodbUser)
                .mongodbPassword(mongodbPassword)
                .name(name)
                .serverTimeZone(serverTimeZone.toString())
                .debeziumProperties(dbzProperties)
                .deserializer(deserializer);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new MongodbTableSource(
                physicalSchema,
                mongodbName,
                mongodbHosts,
                database,
                tableName,
                mongodbUser,
                mongodbPassword,
                name,
                serverTimeZone,
                dbzProperties
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongodbTableSource that = (MongodbTableSource) o;
        return
                Objects.equals(physicalSchema, that.physicalSchema) &&
                        Objects.equals(mongodbName, that.mongodbName) &&
                        Objects.equals(mongodbHosts, that.mongodbHosts) &&
                        Objects.equals(database, that.database) &&
                        Objects.equals(mongodbUser, that.mongodbUser) &&
                        Objects.equals(mongodbPassword, that.mongodbPassword) &&
                        Objects.equals(name, that.name) &&
                        Objects.equals(tableName, that.tableName) &&
                        Objects.equals(serverTimeZone, that.serverTimeZone) &&
                        Objects.equals(dbzProperties, that.dbzProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalSchema, mongodbName, mongodbHosts, database, mongodbUser, mongodbPassword, name, tableName, serverTimeZone, dbzProperties);
    }

    @Override
    public String asSummaryString() {
        return "MONGODB-CDC";
    }
}
