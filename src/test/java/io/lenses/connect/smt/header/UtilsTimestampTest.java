package io.lenses.connect.smt.header;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UtilsTimestampTest {

    public static final String TIMESTAMP = "2024-08-16T04:30:00.232Z";
    public static final String PRECISION = "milliseconds";

    @Test
    void convertToTimestampShouldWritePropsOnFailure() {
        PropsFormatter propsFormatter = new PropsFormatter(new SimpleConfig(new ConfigDef(), Map.of("some", "props", "for", "2" ) ));
        DataException dataException = assertThrows(DataException.class, () -> Utils.convertToTimestamp(
                TIMESTAMP,
                PRECISION,
                Optional.empty(),
                ZoneId.of("UTC"),
                Optional.of(propsFormatter)
        ));
        assertEquals("Expected a long, but found 2024-08-16T04:30:00.232Z. Props: {some: \"props\", for: \"2\"}",dataException.getMessage());
    }

    @Test
    void convertToTimestampShouldNotFailWhenNoPropsFormatter() {
        DataException dataException = assertThrows(DataException.class, () -> Utils.convertToTimestamp(
                TIMESTAMP,
                PRECISION,
                Optional.empty(),
                ZoneId.of("UTC")
        ));
        assertEquals("Expected a long, but found 2024-08-16T04:30:00.232Z. Props: (No props formatter)",dataException.getMessage());
    }

}