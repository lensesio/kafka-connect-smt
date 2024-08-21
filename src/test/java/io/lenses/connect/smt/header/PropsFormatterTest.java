package io.lenses.connect.smt.header;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PropsFormatterTest {

    @Test
    void singleEntry() {
        Map<String, Object> props = Map.of("something", "else");
        PropsFormatter writer = new PropsFormatter(new SimpleConfig(new ConfigDef(), props));
        assertEquals("{something: \"else\"}", writer.apply());
    }

    @Test
    void multipleEntries() {
        Map<String, Object> props = Map.of("first", "item", "something", "else");
        PropsFormatter writer = new PropsFormatter(new SimpleConfig(new ConfigDef(), props));
        assertEquals("{first: \"item\", something: \"else\"}", writer.apply());
    }
}