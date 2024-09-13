package io.lenses.connect.smt.header;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import static org.junit.jupiter.api.Assertions.*;

class MultiDateTimeFormatterTest {

    @Test
    void testFormatWithValidDateString() {
        MultiDateTimeFormatter formatter = MultiDateTimeFormatter.createDateTimeFormatter(
                List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
                "TestConfig",
                ZoneId.of("UTC")
        );

        Instant expected = Instant.parse("2021-10-01T11:30:00Z");
        Instant result = formatter.format("2021-10-01T11:30:00", ZoneId.of("UTC"));
        assertEquals(expected, result);
    }

    @Test
    void testFormatWithInvalidDateString() {
        MultiDateTimeFormatter formatter = MultiDateTimeFormatter.createDateTimeFormatter(
                List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
                "TestConfig",
                ZoneId.of("UTC")
        );

        assertThrows(DateTimeParseException.class, () -> {
            formatter.format("invalid-date", ZoneId.of("UTC"));
        });
    }

    @Test
    void testFormatWithNullValueAndReturnNowIfNullTrue() {
        MultiDateTimeFormatter formatter = new MultiDateTimeFormatter(
                List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
                List.of(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")),
                true
        );

        Instant result = formatter.format(null, ZoneId.of("UTC"));
        assertNotNull(result);
    }

    @Test
    void testFormatWithNullValueAndReturnNowIfNullFalse() {
        MultiDateTimeFormatter formatter = new MultiDateTimeFormatter(
                List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
                List.of(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")),
                false
        );

        assertThrows(DateTimeParseException.class, () -> {
            formatter.format(null, ZoneId.of("UTC"));
        });
    }

    @Test
    void testGetDisplayPatterns() {
        MultiDateTimeFormatter formatter = MultiDateTimeFormatter.createDateTimeFormatter(
                List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
                "TestConfig",
                Locale.US
        );

        String expected = "yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd HH:mm:ss";
        String result = formatter.getDisplayPatterns();
        assertEquals(expected, result);
    }

    @Test
void testFormatWithEmptyListOfDateStrings() {
    MultiDateTimeFormatter formatter = new MultiDateTimeFormatter(
            List.of(),
            List.of(),
            false
    );

    assertThrows(DateTimeParseException.class, () -> formatter.format("2021-10-01T11:30:00", ZoneId.of("UTC")));
}

@Test
void testFormatWithMultiplePatternsTargetingFirst() {
    MultiDateTimeFormatter formatter = MultiDateTimeFormatter.createDateTimeFormatter(
            List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
            "TestConfig",
            ZoneId.of("UTC")
    );

    Instant expected = Instant.parse("2021-10-01T11:30:00Z");
    Instant result = formatter.format("2021-10-01T11:30:00", ZoneId.of("UTC"));
    assertEquals(expected, result);
}

@Test
void testFormatWithMultiplePatternsTargetingSecond() {
    MultiDateTimeFormatter formatter = MultiDateTimeFormatter.createDateTimeFormatter(
            List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
            "TestConfig",
            ZoneId.of("UTC")
    );

    Instant expected = Instant.parse("2021-10-01T11:30:00Z");
    Instant result = formatter.format("2021-10-01 11:30:00", ZoneId.of("UTC"));
    assertEquals(expected, result);
}
}