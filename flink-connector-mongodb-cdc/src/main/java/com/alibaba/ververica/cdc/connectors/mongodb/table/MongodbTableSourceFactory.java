package com.alibaba.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Set;

public class MongodbTableSourceFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}
