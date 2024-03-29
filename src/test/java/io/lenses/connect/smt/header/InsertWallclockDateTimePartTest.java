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
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link io.lenses.connect.smt.header.InsertWallclockDateTimePart}. */
public class InsertWallclockDateTimePartTest {
  @Test
  public void testInsertYear() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "year");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-01T00:00:00.000Z"));

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
    assertEquals("2020", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertMonth() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "month");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-01T00:00:00.000Z"));

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
    assertEquals("1", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertDay() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "day");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-13T00:00:00.000Z"));

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
    assertEquals("13", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertHour() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "hour");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-01T13:00:00.000Z"));

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
    assertEquals("13", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertHourAndTimezoneIsKalkota() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "hour");
    configs.put("timezone", "Asia/Kolkata");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-01T13:00:00.000Z"));

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
    assertEquals("18", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertYearAndTimezoneIsKalkota() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "year");
    configs.put("timezone", "Asia/Kolkata");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-12-31T23:00:00.000Z"));

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
    assertEquals("2021", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertMinute() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "minute");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-01T00:13:00.000Z"));

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
    assertEquals("13", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInsertSecond() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "second");
    transformer.configure(configs);
    transformer.setInstantSupplier(() -> Instant.parse("2020-01-01T00:00:13.000Z"));

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
    assertEquals("13", transformed.headers().lastWithName("wallclock").value());
  }

  @Test
  public void testInvalidPartRaisedConfigException() {
    InsertWallclockDateTimePart<SourceRecord> transformer = new InsertWallclockDateTimePart<>();
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("date.time.part", "invalid");
    try {
      transformer.configure(configs);

    } catch (ConfigException e) {
      assertEquals(
          "Invalid 'date.time.part': invalid. "
              + "Valid values are: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND",
          e.getMessage());
    } catch (Exception e) {
      fail("Should raise ConfigException");
    }
  }
}
