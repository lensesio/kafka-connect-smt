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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link InsertRollingWallclock}. */
public class InsertRollingWallclockTest {
  @Test
  public void testRollingWindowEvery15Minutes() {
    ArrayList<Tuple3<String, Integer, String>> scenarios = new ArrayList<>();

    scenarios.add(new Tuple3<>(("2020-01-01T01:00:00.999Z"), 15, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:00:01.000Z"), 15, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:14:59.000Z"), 15, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:15:00.000Z"), 15, "2020-01-01 01:15"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:15:01.000Z"), 15, "2020-01-01 01:15"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:29:59.000Z"), 15, "2020-01-01 01:15"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:30:00.000Z"), 15, "2020-01-01 01:30"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:30:01.000Z"), 15, "2020-01-01 01:30"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:44:59.000Z"), 15, "2020-01-01 01:30"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:45:00.000Z"), 15, "2020-01-01 01:45"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:45:01.000Z"), 15, "2020-01-01 01:45"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:59:59.000Z"), 15, "2020-01-01 01:45"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "format");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("window.size", scenario.second.toString());
          configs.put("window.type", "minutes");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          assertEquals(actual, scenario.third);
        });
    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "epoch");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("window.size", scenario.second.toString());
          configs.put("window.type", "minutes");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          final String expected = convertToEpochMillis(scenario.third, "yyyy-MM-dd HH:mm");
          assertEquals(actual, expected);
        });
  }

  @Test
  public void testRollingWindowEvery5Minutes() {
    ArrayList<Tuple3<String, Integer, String>> scenarios = new ArrayList<>();

    scenarios.add(new Tuple3<>(("2020-01-01T01:00:00.999Z"), 5, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:00:01.000Z"), 5, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:04:59.000Z"), 5, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:05:00.000Z"), 5, "2020-01-01 01:05"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:05:01.000Z"), 5, "2020-01-01 01:05"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:09:59.000Z"), 5, "2020-01-01 01:05"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:10:00.000Z"), 5, "2020-01-01 01:10"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:10:01.000Z"), 5, "2020-01-01 01:10"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:14:59.000Z"), 5, "2020-01-01 01:10"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:15:00.000Z"), 5, "2020-01-01 01:15"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:15:01.000Z"), 5, "2020-01-01 01:15"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:19:59.000Z"), 5, "2020-01-01 01:15"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:00.000Z"), 5, "2020-01-01 01:20"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:01.000Z"), 5, "2020-01-01 01:20"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:24:59.000Z"), 5, "2020-01-01 01:20"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:25:00.000Z"), 5, "2020-01-01 01:25"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:25:01.000Z"), 5, "2020-01-01 01:25"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:29:59.000Z"), 5, "2020-01-01 01:25"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:30:00.000Z"), 5, "2020-01-01 01:30"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:30:01.000Z"), 5, "2020-01-01 01:30"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:34:59.000Z"), 5, "2020-01-01 01:30"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:35:00.000Z"), 5, "2020-01-01 01:35"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:35:01.000Z"), 5, "2020-01-01 01:35"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:39:59.000Z"), 5, "2020-01-01 01:35"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:40:00.000Z"), 5, "2020-01-01 01:40"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:40:01.000Z"), 5, "2020-01-01 01:40"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:44:59.000Z"), 5, "2020-01-01 01:40"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:45:00.000Z"), 5, "2020-01-01 01:45"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:45:01.000Z"), 5, "2020-01-01 01:45"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:49:59.000Z"), 5, "2020-01-01 01:45"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:50:00.000Z"), 5, "2020-01-01 01:50"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:50:01.000Z"), 5, "2020-01-01 01:50"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:54:59.000Z"), 5, "2020-01-01 01:50"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:55:00.000Z"), 5, "2020-01-01 01:55"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:55:01.000Z"), 5, "2020-01-01 01:55"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:59:59.000Z"), 5, "2020-01-01 01:55"));
    scenarios.add(new Tuple3<>(("2020-01-01T02:00:00.000Z"), 5, "2020-01-01 02:00"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "format");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "minutes");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          assertEquals(actual, scenario.third);
        });

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "epoch");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "minutes");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          final String expected = convertToEpochMillis(scenario.third, "yyyy-MM-dd HH:mm");
          assertEquals(actual, expected);
        });
  }

  @Test
  public void testFormattedWithRollingWindowOf1Hour() {
    ArrayList<Tuple3<String, Integer, String>> scenarios = new ArrayList<>();
    scenarios.add(new Tuple3<>(("2020-01-01T01:19:59.999Z"), 1, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:00.000Z"), 1, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:01.000Z"), 1, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:59:59.000Z"), 1, "2020-01-01 01:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T02:00:00.000Z"), 1, "2020-01-01 02:00"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "format");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "hours");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          assertEquals(actual, scenario.third);
        });

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "epoch");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "hours");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          final String expected = convertToEpochMillis(scenario.third, "yyyy-MM-dd HH:mm");
          assertEquals(actual, expected);
        });
  }

  @Test
  public void testRollingWindowOf3Hours() {
    ArrayList<Tuple3<String, Integer, String>> scenarios = new ArrayList<>();
    scenarios.add(new Tuple3<>(("2020-01-01T01:19:59.999Z"), 3, "2020-01-01 00:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:00.000Z"), 3, "2020-01-01 00:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:01.000Z"), 3, "2020-01-01 00:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T02:19:59.000Z"), 3, "2020-01-01 00:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T02:20:00.000Z"), 3, "2020-01-01 00:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T02:20:01.000Z"), 3, "2020-01-01 00:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T03:19:59.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T03:20:00.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T03:20:01.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T04:19:59.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T04:20:00.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T04:20:01.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T05:19:59.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T05:20:00.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T05:59:59.000Z"), 3, "2020-01-01 03:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T06:00:00.000Z"), 3, "2020-01-01 06:00"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "format");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "hours");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          assertEquals(actual, scenario.third);
        });

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "epoch");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "hours");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          final String expected = convertToEpochMillis(scenario.third, "yyyy-MM-dd HH:mm");
          assertEquals(actual, expected);
        });
  }

  @Test
  public void testRollingWindowEvery12Seconds() {
    ArrayList<Tuple3<String, Integer, String>> scenarios = new ArrayList<>();
    scenarios.add(new Tuple3<>(("2020-01-01T01:19:59.000Z"), 12, "2020-01-01 01:19:48"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:00.000Z"), 12, "2020-01-01 01:20:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:01.000Z"), 12, "2020-01-01 01:20:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:02.000Z"), 12, "2020-01-01 01:20:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:11.999Z"), 12, "2020-01-01 01:20:00"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:12.000Z"), 12, "2020-01-01 01:20:12"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:13.000Z"), 12, "2020-01-01 01:20:12"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:23.999Z"), 12, "2020-01-01 01:20:12"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:24.000Z"), 12, "2020-01-01 01:20:24"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:25.000Z"), 12, "2020-01-01 01:20:24"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:35.999Z"), 12, "2020-01-01 01:20:24"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:36.000Z"), 12, "2020-01-01 01:20:36"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:37.000Z"), 12, "2020-01-01 01:20:36"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:47.999Z"), 12, "2020-01-01 01:20:36"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:48.000Z"), 12, "2020-01-01 01:20:48"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:49.000Z"), 12, "2020-01-01 01:20:48"));
    scenarios.add(new Tuple3<>(("2020-01-01T01:20:59.999Z"), 12, "2020-01-01 01:20:48"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "format");
          configs.put("format", "yyyy-MM-dd HH:mm:ss");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "seconds");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          assertEquals(actual, scenario.third);
        });

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.name", "wallclock");
          configs.put("value.type", "epoch");
          configs.put("format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "seconds");
          final InsertRollingWallclock<SourceRecord> transformer = new InsertRollingWallclock<>();
          transformer.configure(configs);
          transformer.setInstantF(() -> Instant.parse(scenario.first));

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
                  Instant.parse(scenario.first).toEpochMilli(),
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actual = transformed.headers().lastWithName("wallclock").value().toString();
          final String expected = convertToEpochMillis(scenario.third, "yyyy-MM-dd HH:mm:ss");
          assertEquals(actual, expected);
        });
  }

  private static String convertToEpochMillis(String date, String pattern) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    LocalDateTime localDateTime = LocalDateTime.parse(date, formatter);
    return String.valueOf(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
  }

  static class Tuple3<A, B, C> {
    private final A first;
    private final B second;
    private final C third;

    public Tuple3(A first, B second, C third) {
      this.first = first;
      this.second = second;
      this.third = third;
    }
  }
}
