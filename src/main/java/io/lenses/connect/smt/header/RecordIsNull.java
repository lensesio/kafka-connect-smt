package io.lenses.connect.smt.header;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.util.Map;

public class RecordIsNull<R extends ConnectRecord<R>> implements Predicate<R>, Versioned {

    public static final String OVERVIEW_DOC = "A predicate which is true for records which are tombstones (i.e. have null value).";
    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        return record == null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public String toString() {
        return "RecordIsTombstone{}";
    }
}
