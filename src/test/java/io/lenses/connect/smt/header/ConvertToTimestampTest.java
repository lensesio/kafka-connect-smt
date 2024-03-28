package io.lenses.connect.smt.header;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

public class ConvertToTimestampTest {

  @Test
  public void convertToTimestampReturnsCurrentTimeWhenValueIsNull() {
    Instant result =
        Utils.convertToTimestamp(null, "seconds", Optional.empty(), ZoneId.systemDefault());
    assertNotNull(result);
  }

  @Test
  public void convertToTimestampReturnsSameInstantWhenValueIsInstant() {
    Instant instant = Instant.now();
    Instant result =
        Utils.convertToTimestamp(instant, "seconds", Optional.empty(), ZoneId.systemDefault());
    assertEquals(instant, result);
  }

  @Test
  public void convertToTimestampReturnsCorrectInstantWhenValueIsLong() {
    Long value = 1633097000L; // corresponds to 2021-10-01T11:30:00Z
    Instant result =
        Utils.convertToTimestamp(value, "seconds", Optional.empty(), ZoneId.systemDefault());
    assertEquals(Instant.ofEpochSecond(value), result);
  }

  @Test
  public void convertToTimestampReturnsCorrectInstantWhenValueIsString() {
    String value = "2021-10-01T11:30:00Z";
    Instant result =
        Utils.convertToTimestamp(
            value,
            "seconds",
            Optional.of(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZZZZ").withZone(UTC)),
            UTC);
    assertEquals(Instant.parse(value), result);
  }

  @Test
  public void convertToTimestampThrowsDataExceptionWhenValueIsInvalidString() {
    String value = "invalid";
    assertThrows(
        DataException.class,
        () ->
            Utils.convertToTimestamp(
                value,
                "seconds",
                Optional.of(
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZZZZ").withZone(UTC)),
                UTC));
  }

  @Test
  public void convertToTimestampReturnsCorrectInstantWhenValueIsEpochAndPrecisionIsMicros() {
    Long value = 1633097000000000L; // corresponds to 2021-10-01T11:30:00Z
    Instant result =
        Utils.convertToTimestamp(value, "microseconds", Optional.empty(), ZoneId.systemDefault());
    assertEquals(Instant.ofEpochSecond(1633097000L, 0), result);
  }

  @Test
  public void convertToTimestampReturnsCorrectInstantWhenValueIsEpochAndPrecisionIsNanos() {
    Long value = 1633097000000000L; // corresponds to 2021-10-01T11:30:00Z
    Instant result =
        Utils.convertToTimestamp(value, "nanoseconds", Optional.empty(), ZoneId.systemDefault());
    // Convert nanoseconds to seconds and add to epoch second
    Instant expected = Instant.ofEpochSecond(value / 1_000_000_000L);
    assertEquals(expected, result);
  }

  @Test
  public void convertToTimestampReturnsCorrectInstantWhenValueIsEpochAndPrecisionIsMillis() {
    Long value = 1633097000000L; // corresponds to 2021-10-01T11:30:00Z
    Instant result =
        Utils.convertToTimestamp(value, "milliseconds", Optional.empty(), ZoneId.systemDefault());
    assertEquals(Instant.ofEpochSecond(1633097000L, 0), result);
  }

  @Test
  public void convertToTimestampReturnsCorrectInstantWhenValueIsEpochAndPrecisionIsSeconds() {
    Long value = 1633097000L; // corresponds to 2021-10-01T11:30:00Z
    Instant result =
        Utils.convertToTimestamp(value, "seconds", Optional.empty(), ZoneId.systemDefault());
    assertEquals(Instant.ofEpochSecond(1633097000L, 0), result);
  }
}
