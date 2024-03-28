/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at: http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package io.lenses.connect.smt.header;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link InsertFieldTimestampHeadersTest}. */
public class InsertFieldTimestampHeadersTest {

  @Test
  public void testAllHeaders() {
    InsertFieldTimestampHeaders<SinkRecord> transformer = new InsertFieldTimestampHeaders<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.prefix.name", "wallclock.");
    configs.put("field", "_value");
    transformer.configure(configs);

    Long expected = Instant.parse("2020-01-05T11:21:04.000Z").toEpochMilli();
    final Headers headers = new ConnectHeaders();
    final SinkRecord record =
        new SinkRecord(
            "topic",
            0,
            Schema.STRING_SCHEMA,
            null,
            Schema.INT64_SCHEMA,
            expected,
            0,
            expected,
            TimestampType.LOG_APPEND_TIME,
            headers);
    final SinkRecord transformed = transformer.apply(record);
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
    InsertFieldTimestampHeaders<SourceRecord> transformer = new InsertFieldTimestampHeaders<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.prefix.name", "wallclock.");
    configs.put("timezone", "Asia/Kolkata");
    configs.put("field", "_value");
    transformer.configure(configs);

    long expected = Instant.parse("2020-01-05T11:21:04.000Z").toEpochMilli();
    final Headers headers = new ConnectHeaders();
    final SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.INT64_SCHEMA,
            expected,
            expected,
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
    InsertFieldTimestampHeaders<SourceRecord> transformer = new InsertFieldTimestampHeaders<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.prefix.name", "wallclock.");
    configs.put("date.format", "yyyy-dd-MM");
    configs.put("field", "_value");
    transformer.configure(configs);

    final Headers headers = new ConnectHeaders();
    long expected = Instant.parse("2020-01-05T11:21:04.000Z").toEpochMilli();
    final SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.INT64_SCHEMA,
            expected,
            expected,
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
