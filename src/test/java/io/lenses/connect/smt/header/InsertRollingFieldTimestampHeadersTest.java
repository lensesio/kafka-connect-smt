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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link InsertRollingRecordTimestampHeaders}. */
public class InsertRollingFieldTimestampHeadersTest {

  @Test
  public void testRollingWindowEvery15Minutes() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();

    scenarios.add(new Tuple5<>(("2020-01-01T01:00:00.999Z"), 15, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:00:01.000Z"), 15, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:14:59.000Z"), 15, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:00.000Z"), 15, "2020-01-01 01:15", "01", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:01.000Z"), 15, "2020-01-01 01:15", "01", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:29:59.000Z"), 15, "2020-01-01 01:15", "01", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:00.000Z"), 15, "2020-01-01 01:30", "01", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:01.000Z"), 15, "2020-01-01 01:30", "01", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:44:59.000Z"), 15, "2020-01-01 01:30", "01", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:00.000Z"), 15, "2020-01-01 01:45", "01", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:01.000Z"), 15, "2020-01-01 01:45", "01", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:59:59.000Z"), 15, "2020-01-01 01:45", "01", "45"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.prefix.name", "wallclock_");
          configs.put("date.format", "yyyy-MM-dd HH:mm");
          configs.put("window.size", scenario.second.toString());
          configs.put("window.type", "minutes");
          configs.put("field", "_value");

          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
          final SourceRecord record =
              new SourceRecord(
                  null, null, "topic", 0, Schema.STRING_SCHEMA, "key", null, expected, 0L, headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate =
              transformed.headers().lastWithName("wallclock_date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualHour =
              transformed.headers().lastWithName("wallclock_hour").value().toString();
          assertEquals(actualHour, scenario.fourth);
          final String actualMinute =
              transformed.headers().lastWithName("wallclock_minute").value().toString();
          assertEquals(actualMinute, scenario.fifth);
        });
  }

  @Test
  public void testRollingWindowEvery15MinutesAndTimezoneSetToKalkota() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();

    // the first param to the Tuple5 is UTC. the third, fourth and figth arguments should be adapted
    // to the Kalkota timezone
    scenarios.add(new Tuple5<>(("2020-01-01T01:00:00.999Z"), 15, "2020-01-01 06:30", "06", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:00:01.000Z"), 15, "2020-01-01 06:30", "06", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:14:59.000Z"), 15, "2020-01-01 06:30", "06", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:00.000Z"), 15, "2020-01-01 06:45", "06", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:01.000Z"), 15, "2020-01-01 06:45", "06", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:29:59.000Z"), 15, "2020-01-01 06:45", "06", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:00.000Z"), 15, "2020-01-01 07:00", "07", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:01.000Z"), 15, "2020-01-01 07:00", "07", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:44:59.000Z"), 15, "2020-01-01 07:00", "07", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:00.000Z"), 15, "2020-01-01 07:15", "07", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:01.000Z"), 15, "2020-01-01 07:15", "07", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:59:59.000Z"), 15, "2020-01-01 07:15", "07", "15"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.prefix.name", "wallclock_");
          configs.put("date.format", "yyyy-MM-dd HH:mm");
          configs.put("window.size", scenario.second.toString());
          configs.put("window.type", "minutes");
          configs.put("timezone", "Asia/Kolkata");
          configs.put("field", "_value");
          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
          final SourceRecord record =
              new SourceRecord(
                  null, null, "topic", 0, Schema.STRING_SCHEMA, "key", null, expected, 0L, headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate =
              transformed.headers().lastWithName("wallclock_date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualHour =
              transformed.headers().lastWithName("wallclock_hour").value().toString();
          assertEquals(actualHour, scenario.fourth);
          final String actualMinute =
              transformed.headers().lastWithName("wallclock_minute").value().toString();
          assertEquals(actualMinute, scenario.fifth);
        });
  }

  @Test
  public void testRollingWindowEvery15MinutesAndTimezoneIsParis() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();

    scenarios.add(new Tuple5<>(("2020-01-01T01:00:00.999Z"), 15, "2020-01-01 02:00", "02", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:00:01.000Z"), 15, "2020-01-01 02:00", "02", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:14:59.000Z"), 15, "2020-01-01 02:00", "02", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:00.000Z"), 15, "2020-01-01 02:15", "02", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:01.000Z"), 15, "2020-01-01 02:15", "02", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:29:59.000Z"), 15, "2020-01-01 02:15", "02", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:00.000Z"), 15, "2020-01-01 02:30", "02", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:01.000Z"), 15, "2020-01-01 02:30", "02", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:44:59.000Z"), 15, "2020-01-01 02:30", "02", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:00.000Z"), 15, "2020-01-01 02:45", "02", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:01.000Z"), 15, "2020-01-01 02:45", "02", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:59:59.000Z"), 15, "2020-01-01 02:45", "02", "45"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.prefix.name", "_");
          configs.put("date.format", "yyyy-MM-dd HH:mm");
          configs.put("window.size", scenario.second.toString());
          configs.put("window.type", "minutes");
          configs.put("timezone", "Europe/Paris");
          configs.put("field", "_value");
          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
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
                  0L,
                  headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate = transformed.headers().lastWithName("_date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualHour = transformed.headers().lastWithName("_hour").value().toString();
          assertEquals(actualHour, scenario.fourth);
          final String actualMinute =
              transformed.headers().lastWithName("_minute").value().toString();
          assertEquals(actualMinute, scenario.fifth);
        });
  }

  @Test
  public void testRollingWindowEvery5Minutes() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();

    scenarios.add(new Tuple5<>(("2020-01-01T01:00:00.999Z"), 5, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:00:01.000Z"), 5, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:04:59.000Z"), 5, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:05:00.000Z"), 5, "2020-01-01 01:05", "01", "05"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:05:01.000Z"), 5, "2020-01-01 01:05", "01", "05"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:09:59.000Z"), 5, "2020-01-01 01:05", "01", "05"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:10:00.000Z"), 5, "2020-01-01 01:10", "01", "10"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:10:01.000Z"), 5, "2020-01-01 01:10", "01", "10"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:14:59.000Z"), 5, "2020-01-01 01:10", "01", "10"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:00.000Z"), 5, "2020-01-01 01:15", "01", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:15:01.000Z"), 5, "2020-01-01 01:15", "01", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:19:59.000Z"), 5, "2020-01-01 01:15", "01", "15"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:20:00.000Z"), 5, "2020-01-01 01:20", "01", "20"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:20:01.000Z"), 5, "2020-01-01 01:20", "01", "20"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:24:59.000Z"), 5, "2020-01-01 01:20", "01", "20"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:25:00.000Z"), 5, "2020-01-01 01:25", "01", "25"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:25:01.000Z"), 5, "2020-01-01 01:25", "01", "25"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:29:59.000Z"), 5, "2020-01-01 01:25", "01", "25"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:00.000Z"), 5, "2020-01-01 01:30", "01", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:30:01.000Z"), 5, "2020-01-01 01:30", "01", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:34:59.000Z"), 5, "2020-01-01 01:30", "01", "30"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:35:00.000Z"), 5, "2020-01-01 01:35", "01", "35"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:35:01.000Z"), 5, "2020-01-01 01:35", "01", "35"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:39:59.000Z"), 5, "2020-01-01 01:35", "01", "35"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:40:00.000Z"), 5, "2020-01-01 01:40", "01", "40"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:40:01.000Z"), 5, "2020-01-01 01:40", "01", "40"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:44:59.000Z"), 5, "2020-01-01 01:40", "01", "40"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:00.000Z"), 5, "2020-01-01 01:45", "01", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:45:01.000Z"), 5, "2020-01-01 01:45", "01", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:49:59.000Z"), 5, "2020-01-01 01:45", "01", "45"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:50:00.000Z"), 5, "2020-01-01 01:50", "01", "50"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:50:01.000Z"), 5, "2020-01-01 01:50", "01", "50"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:54:59.000Z"), 5, "2020-01-01 01:50", "01", "50"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:55:00.000Z"), 5, "2020-01-01 01:55", "01", "55"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:55:01.000Z"), 5, "2020-01-01 01:55", "01", "55"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:59:59.000Z"), 5, "2020-01-01 01:55", "01", "55"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:00:00.000Z"), 5, "2020-01-01 02:00", "02", "00"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.prefix.name", "wallclock_");
          configs.put("date.format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "minutes");
          configs.put("field", "_value");
          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
          final SourceRecord record =
              new SourceRecord(
                  null, null, "topic", 0, Schema.STRING_SCHEMA, "key", null, expected, 0L, headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate =
              transformed.headers().lastWithName("wallclock_date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualHour =
              transformed.headers().lastWithName("wallclock_hour").value().toString();
          assertEquals(actualHour, scenario.fourth);

          final String actualMinute =
              transformed.headers().lastWithName("wallclock_minute").value().toString();
          assertEquals(actualMinute, scenario.fifth);
        });
  }

  @Test
  public void testFormattedWithRollingWindowOf1Hour() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();
    scenarios.add(new Tuple5<>(("2020-01-01T01:19:59.999Z"), 1, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:20:00.000Z"), 1, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:20:01.000Z"), 1, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:59:59.000Z"), 1, "2020-01-01 01:00", "01", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:00:00.000Z"), 1, "2020-01-01 02:00", "02", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:15:00.000Z"), 1, "2020-01-01 02:00", "02", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:59:59.999Z"), 1, "2020-01-01 02:00", "02", "00"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("header.prefix.name", "wallclock_");
          configs.put("date.format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "hours");
          configs.put("field", "_value");
          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
          final SourceRecord record =
              new SourceRecord(
                  null, null, "topic", 0, Schema.STRING_SCHEMA, "key", null, expected, 0L, headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate =
              transformed.headers().lastWithName("wallclock_date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualHour =
              transformed.headers().lastWithName("wallclock_hour").value().toString();
          assertEquals(actualHour, scenario.fourth);
        });
  }

  @Test
  public void testRollingWindowOf3Hours() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();
    scenarios.add(new Tuple5<>(("2020-01-01T01:19:59.999Z"), 3, "2020-01-01 00:00", "00", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:20:00.000Z"), 3, "2020-01-01 00:00", "00", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T01:20:01.000Z"), 3, "2020-01-01 00:00", "00", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:19:59.000Z"), 3, "2020-01-01 00:00", "00", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:20:00.000Z"), 3, "2020-01-01 00:00", "00", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T02:20:01.000Z"), 3, "2020-01-01 00:00", "00", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T03:19:59.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T03:20:00.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T03:20:01.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T04:19:59.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T04:20:00.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T04:20:01.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T05:19:59.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T05:20:00.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T05:59:59.000Z"), 3, "2020-01-01 03:00", "03", "00"));
    scenarios.add(new Tuple5<>(("2020-01-01T06:00:00.000Z"), 3, "2020-01-01 06:00", "06", "00"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("date.format", "yyyy-MM-dd HH:mm");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "hours");
          configs.put("field", "_value");
          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
          final SourceRecord record =
              new SourceRecord(
                  null, null, "topic", 0, Schema.STRING_SCHEMA, "key", null, expected, 0L, headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate = transformed.headers().lastWithName("date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualHour = transformed.headers().lastWithName("hour").value().toString();
          assertEquals(actualHour, scenario.fourth);
        });
  }

  @Test
  public void testRollingWindowEvery12Seconds() {
    ArrayList<Tuple5<String, Integer, String, String, String>> scenarios = new ArrayList<>();
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:19:59.000Z"), 12, "2020-01-01 01:19:48", "19", "48"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:00.000Z"), 12, "2020-01-01 01:20:00", "20", "00"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:01.000Z"), 12, "2020-01-01 01:20:00", "20", "00"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:02.000Z"), 12, "2020-01-01 01:20:00", "20", "00"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:11.999Z"), 12, "2020-01-01 01:20:00", "20", "00"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:12.000Z"), 12, "2020-01-01 01:20:12", "20", "12"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:13.000Z"), 12, "2020-01-01 01:20:12", "20", "12"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:23.999Z"), 12, "2020-01-01 01:20:12", "20", "12"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:24.000Z"), 12, "2020-01-01 01:20:24", "20", "24"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:25.000Z"), 12, "2020-01-01 01:20:24", "20", "24"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:35.999Z"), 12, "2020-01-01 01:20:24", "20", "24"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:36.000Z"), 12, "2020-01-01 01:20:36", "20", "36"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:37.000Z"), 12, "2020-01-01 01:20:36", "20", "36"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:47.999Z"), 12, "2020-01-01 01:20:36", "20", "36"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:48.000Z"), 12, "2020-01-01 01:20:48", "20", "48"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:49.000Z"), 12, "2020-01-01 01:20:48", "20", "48"));
    scenarios.add(
        new Tuple5<>(("2020-01-01T01:20:59.999Z"), 12, "2020-01-01 01:20:48", "20", "48"));

    scenarios.forEach(
        scenario -> {
          Map<String, String> configs = new HashMap<>();
          configs.put("date.format", "yyyy-MM-dd HH:mm:ss");
          configs.put("rolling.window.size", scenario.second.toString());
          configs.put("rolling.window.type", "seconds");
          configs.put("field", "_value");
          final InsertRollingFieldTimestampHeaders<SourceRecord> transformer =
              new InsertRollingFieldTimestampHeaders<>();
          transformer.configure(configs);

          final Headers headers = new ConnectHeaders();
          long expected = Instant.parse(scenario.first).toEpochMilli();
          final SourceRecord record =
              new SourceRecord(
                  null, null, "topic", 0, Schema.STRING_SCHEMA, "key", null, expected, 0L, headers);
          final SourceRecord transformed = transformer.apply(record);
          final String actualDate = transformed.headers().lastWithName("date").value().toString();
          assertEquals(actualDate, scenario.third);

          final String actualSecond =
              transformed.headers().lastWithName("second").value().toString();
          assertEquals(actualSecond, scenario.fifth);
        });
  }

  static class Tuple5<A, B, C, D, E> {
    private final A first;
    private final B second;
    private final C third;
    private final D fourth;
    private final E fifth;

    public Tuple5(A first, B second, C third, D fourth, E fifth) {
      this.first = first;
      this.second = second;
      this.third = third;
      this.fourth = fourth;
      this.fifth = fifth;
    }
  }
}
