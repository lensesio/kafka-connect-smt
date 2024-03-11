package io.lenses.connect.smt.header;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link io.lenses.connect.smt.header.InsertWallclockHeaders}. */
public class InsertWallclockHeadersTest {

  @Test
  public void testAllHeaders() {
    InsertWallclockHeaders<SourceRecord> transformer = new InsertWallclockHeaders<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.prefix.name", "wallclock.");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-05T11:21:04.000Z"));

    final Headers headers = new ConnectHeaders();
    final SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "value",
            System.currentTimeMillis(),
            headers);
    final SourceRecord transformed = transformer.apply(record);
    assertEquals("2020", transformed.headers().lastWithName("wallclock.year").value());
    assertEquals("01", transformed.headers().lastWithName("wallclock.month").value());
    assertEquals("05", transformed.headers().lastWithName("wallclock.day").value());
    assertEquals("11", transformed.headers().lastWithName("wallclock.hour").value());
    assertEquals("21", transformed.headers().lastWithName("wallclock.minute").value());
    assertEquals("04", transformed.headers().lastWithName("wallclock.second").value());
    assertEquals("2020-01-05", transformed.headers().lastWithName("wallclock.date").value());
  }

  @Test
  public void testUsingKalkotaTimezone() {
    InsertWallclockHeaders<SourceRecord> transformer = new InsertWallclockHeaders<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.prefix.name", "wallclock.");
    configs.put("timezone", "Asia/Kolkata");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-05T11:21:04.000Z"));

    final Headers headers = new ConnectHeaders();
    final SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "value",
            System.currentTimeMillis(),
            headers);
    final SourceRecord transformed = transformer.apply(record);
    assertEquals("2020", transformed.headers().lastWithName("wallclock.year").value());
    assertEquals("01", transformed.headers().lastWithName("wallclock.month").value());
    assertEquals("05", transformed.headers().lastWithName("wallclock.day").value());
    assertEquals("16", transformed.headers().lastWithName("wallclock.hour").value());
    assertEquals("51", transformed.headers().lastWithName("wallclock.minute").value());
    assertEquals("04", transformed.headers().lastWithName("wallclock.second").value());
    assertEquals("2020-01-05", transformed.headers().lastWithName("wallclock.date").value());
  }

  @Test
  public void changeDatePattern() {
    InsertWallclockHeaders<SourceRecord> transformer = new InsertWallclockHeaders<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.prefix.name", "wallclock.");
    configs.put("date.format", "yyyy-dd-MM");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-05T11:21:04.000Z"));

    final Headers headers = new ConnectHeaders();
    final SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "value",
            System.currentTimeMillis(),
            headers);
    final SourceRecord transformed = transformer.apply(record);
    assertEquals("2020", transformed.headers().lastWithName("wallclock.year").value());
    assertEquals("01", transformed.headers().lastWithName("wallclock.month").value());
    assertEquals("05", transformed.headers().lastWithName("wallclock.day").value());
    assertEquals("11", transformed.headers().lastWithName("wallclock.hour").value());
    assertEquals("21", transformed.headers().lastWithName("wallclock.minute").value());
    assertEquals("04", transformed.headers().lastWithName("wallclock.second").value());
    assertEquals("2020-05-01", transformed.headers().lastWithName("wallclock.date").value());
  }
}
