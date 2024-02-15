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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Test for {@link TimestampConverter}. */
public class TimestampConverterTest {
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static final Calendar EPOCH;
  private static final Calendar TIME;
  private static final Calendar DATE;
  private static final Calendar DATE_PLUS_TIME;
  private static final long DATE_PLUS_TIME_UNIX;
  private static final long DATE_PLUS_TIME_UNIX_MICROS;
  private static final long DATE_PLUS_TIME_UNIX_NANOS;
  private static final long DATE_PLUS_TIME_UNIX_SECONDS;
  private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS zzz";
  private static final String DATE_PLUS_TIME_STRING;

  static {
    EPOCH = GregorianCalendar.getInstance(UTC);
    EPOCH.setTimeInMillis(0L);

    TIME = GregorianCalendar.getInstance(UTC);
    TIME.setTimeInMillis(0L);
    TIME.add(Calendar.MILLISECOND, 1234);

    DATE = GregorianCalendar.getInstance(UTC);
    DATE.setTimeInMillis(0L);
    DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
    DATE.add(Calendar.DATE, 1);

    DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
    DATE_PLUS_TIME.setTimeInMillis(0L);
    DATE_PLUS_TIME.add(Calendar.DATE, 1);
    DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);
    // 86 401 234 milliseconds
    DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
    // 86 401 234 123 microseconds
    DATE_PLUS_TIME_UNIX_MICROS = DATE_PLUS_TIME_UNIX * 1000 + 123;
    // 86 401 234 123 456 nanoseconds
    DATE_PLUS_TIME_UNIX_NANOS = DATE_PLUS_TIME_UNIX_MICROS * 1000 + 456;
    // 86401 seconds
    DATE_PLUS_TIME_UNIX_SECONDS = DATE_PLUS_TIME.getTimeInMillis() / 1000;
    DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";
  }

  // Configuration

  @Test
  void testConfigNoTargetType() {
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    assertThrows(
        ConfigException.class, () -> transformer.configure(Collections.<String, String>emptyMap()));
  }

  @Test
  void testConfigInvalidTargetType() {
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    assertThrows(
        ConfigException.class,
        () ->
            transformer.configure(
                Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "invalid")));
  }

  @Test
  void testConfigInvalidUnixPrecision() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "invalid");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    assertThrows(ConfigException.class, () -> transformer.configure(config));
  }

  @Test
  void testConfigValidUnixPrecision() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    assertDoesNotThrow(() -> transformer.configure(config));
  }

  @Test
  void testConfigMissingFormat() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    assertThrows(ConfigException.class, () -> transformer.configure(config));
  }

  @Test
  void testConfigInvalidFormat() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimestampConverter.FORMAT_TO_CONFIG, "bad-format");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    assertThrows(ConfigException.class, () -> transformer.configure(config));
  }

  // Conversions without schemas (most flexible Timestamp -> other types)

  @Test
  void testSchemalessIdentity() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testSchemalessTimestampToDate() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Date");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Date.SCHEMA.type(), header.schema().type());
    assertEquals(DATE.getTime(), header.value());
  }

  @Test
  void testSchemalessTimestampToTime() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Time");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Time.SCHEMA.type(), header.schema().type());
    assertEquals(TIME.getTime(), header.value());
  }

  @Test
  void testSchemalessTimestampToUnix() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Schema.INT64_SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME_UNIX, header.value());
  }

  @Test
  void testSchemalessTimestampToString() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "str_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

    Header header = transformed.headers().lastWithName("str_header");
    assertNotNull(header);

    assertEquals(DATE_PLUS_TIME_STRING, header.value());
  }

  // Conversions without schemas (core types -> most flexible Timestamp format)

  @Test
  void testSchemalessDateToTimestamp() {
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE.getTime(), header.value());
  }

  @Test
  void testSchemalessTimeToTimestamp() {
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(TIME.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(TIME.getTime(), header.value());
  }

  @Test
  void testSchemalessUnixToTimestamp() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME_UNIX));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testSchemalessUnixAsStringToTimestamp() {
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    transformer.configure(config);
    String datePlusTimeUnixString = String.valueOf(DATE_PLUS_TIME_UNIX);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(datePlusTimeUnixString));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testSchemalessStringToTimestamp() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
  }

  // Conversions with schemas (most flexible Timestamp -> other types)

  @Test
  void testWithSchemaIdentity() {
    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaTimestampToDate() {
    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Date");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "dt_header");
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

    final Header header = transformed.headers().lastWithName("dt_header");
    assertNotNull(header);
    assertEquals(DATE.getTime(), header.value());
    assertEquals(Date.SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaTimestampToTime() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Time");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "tm_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

    final Header header = transformed.headers().lastWithName("tm_header");
    assertNotNull(header);
    assertEquals(TIME.getTime(), header.value());
    assertEquals(Time.SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaTimestampToUnix() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "unix_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

    final Header header = transformed.headers().lastWithName("unix_header");
    assertNotNull(header);
    assertEquals(DATE_PLUS_TIME_UNIX, header.value());
    assertEquals(Schema.INT64_SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaTimestampToString() {
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "str_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

    final Header header = transformed.headers().lastWithName("str_header");
    assertNotNull(header);
    assertEquals(DATE_PLUS_TIME_STRING, header.value());
    assertEquals(Schema.STRING_SCHEMA.type(), header.schema().type());
  }

  // Null-value conversions schemaless

  @Test
  void testSchemalessNullValueToString() {
    testSchemalessNullValueConversion("string");
    testSchemalessNullFieldConversion("string");
  }

  @Test
  void testSchemalessNullValueToDate() {
    testSchemalessNullValueConversion("Date");
    testSchemalessNullFieldConversion("Date");
  }

  @Test
  void testSchemalessNullValueToTimestamp() {
    testSchemalessNullValueConversion("Timestamp");
    testSchemalessNullFieldConversion("Timestamp");
  }

  @Test
  void testSchemalessNullValueToUnix() {
    testSchemalessNullValueConversion("unix");
    testSchemalessNullFieldConversion("unix");
  }

  @Test
  void testSchemalessNullValueToTime() {
    testSchemalessNullValueConversion("Time");
    testSchemalessNullFieldConversion("Time");
  }

  private void testSchemalessNullValueConversion(String targetType) {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "a_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(null));

    final Header header = transformed.headers().lastWithName("a_header");
    assertNotNull(header);
    assertNull(header.value());
  }

  private void testSchemalessNullFieldConversion(String targetType) {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "a_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(null));

    final Header header = transformed.headers().lastWithName("a_header");
    assertNotNull(header);
    assertNull(header.value());
  }

  // Conversions with schemas (core types -> most flexible Timestamp format)

  @Test
  void testWithSchemaDateToTimestamp() {
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Date.SCHEMA, DATE.getTime()));

    final Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(DATE.getTime(), header.value());
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaTimeToTimestamp() {
    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Time.SCHEMA, TIME.getTime()));

    final Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(TIME.getTime(), header.value());
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaUnixToTimestamp() {
    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Schema.INT64_SCHEMA, DATE_PLUS_TIME_UNIX));

    final Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
  }

  @Test
  void testWithSchemaStringToTimestamp() {
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(Schema.STRING_SCHEMA, DATE_PLUS_TIME_STRING));

    final Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
  }

  // Null-value conversions with schema

  @Test
  void testWithSchemaNullValueToTimestamp() {
    testWithSchemaNullValueConversion(
        "Timestamp", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullValueConversion(
        "Timestamp",
        TimestampConverter.OPTIONAL_TIME_SCHEMA,
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullValueConversion(
        "Timestamp",
        TimestampConverter.OPTIONAL_DATE_SCHEMA,
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullValueConversion(
        "Timestamp", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullValueConversion(
        "Timestamp",
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA,
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
  }

  @Test
  void testWithSchemaNullFieldToTimestamp() {
    testWithSchemaNullFieldConversion(
        "Timestamp", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Timestamp",
        TimestampConverter.OPTIONAL_TIME_SCHEMA,
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Timestamp",
        TimestampConverter.OPTIONAL_DATE_SCHEMA,
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Timestamp", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Timestamp",
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA,
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
  }

  @Test
  void testWithSchemaNullValueToUnix() {
    testWithSchemaNullValueConversion(
        "unix", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullValueConversion(
        "unix", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullValueConversion(
        "unix", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullValueConversion(
        "unix", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullValueConversion(
        "unix", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  void testWithSchemaNullFieldToUnix() {
    testWithSchemaNullFieldConversion(
        "unix", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullFieldConversion(
        "unix", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullFieldConversion(
        "unix", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullFieldConversion(
        "unix", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    testWithSchemaNullFieldConversion(
        "unix", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  void testWithSchemaNullValueToTime() {
    testWithSchemaNullValueConversion(
        "Time", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullValueConversion(
        "Time", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullValueConversion(
        "Time", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullValueConversion(
        "Time", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullValueConversion(
        "Time",
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA,
        TimestampConverter.OPTIONAL_TIME_SCHEMA);
  }

  @Test
  void testWithSchemaNullFieldToTime() {
    testWithSchemaNullFieldConversion(
        "Time", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Time", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Time", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Time", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Time",
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA,
        TimestampConverter.OPTIONAL_TIME_SCHEMA);
  }

  @Test
  void testWithSchemaNullValueToDate() {
    testWithSchemaNullValueConversion(
        "Date", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullValueConversion(
        "Date", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullValueConversion(
        "Date", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullValueConversion(
        "Date", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullValueConversion(
        "Date",
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA,
        TimestampConverter.OPTIONAL_DATE_SCHEMA);
  }

  @Test
  void testWithSchemaNullFieldToDate() {
    testWithSchemaNullFieldConversion(
        "Date", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Date", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Date", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Date", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    testWithSchemaNullFieldConversion(
        "Date",
        TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA,
        TimestampConverter.OPTIONAL_DATE_SCHEMA);
  }

  @Test
  void testWithSchemaNullValueToString() {
    testWithSchemaNullValueConversion(
        "string", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullValueConversion(
        "string", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullValueConversion(
        "string", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullValueConversion(
        "string", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullValueConversion(
        "string", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
  }

  @Test
  void testWithSchemaNullFieldToString() {
    testWithSchemaNullFieldConversion(
        "string", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullFieldConversion(
        "string", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullFieldConversion(
        "string", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullFieldConversion(
        "string", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    testWithSchemaNullFieldConversion(
        "string", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
  }

  private void testWithSchemaNullValueConversion(
      String targetType, Schema originalSchema, Schema expectedSchema) {
    final Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "a_header");
    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordWithSchema(originalSchema, null));

    final Header header = transformed.headers().lastWithName("a_header");
    assertNotNull(header);
    assertEquals(expectedSchema.type(), header.schema().type());
    assertNull(header.value());
  }

  private void testWithSchemaNullFieldConversion(
      String targetType, Schema originalSchema, Schema expectedSchema) {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "a_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SchemaBuilder structSchema =
        SchemaBuilder.struct().field("ts", originalSchema).field("other", Schema.STRING_SCHEMA);

    Struct original = new Struct(structSchema);
    original.put("ts", null);
    original.put("other", "test");

    // Struct field is null
    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(structSchema.build(), original));

    Header header = transformed.headers().lastWithName("a_header");
    assertNotNull(header);
    assertEquals(expectedSchema, header.schema());
    assertNull(header.value());

    // entire Struct is null
    transformed = transformer.apply(createRecordWithSchema(structSchema.optional().build(), null));

    header = transformed.headers().lastWithName("a_header");
    assertNull(header);
  }

  // Convert field instead of entire key/value

  @Test
  void testSchemalessFieldConversion() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Date");
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);

    Object value = Collections.singletonMap("ts", DATE_PLUS_TIME.getTime());
    SourceRecord transformed = transformer.apply(createRecordSchemaless(value));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(TimestampConverter.OPTIONAL_DATE_SCHEMA, header.schema());
    assertEquals(DATE.getTime(), header.value());
  }

  @Test
  void testWithSchemaFieldConversion() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);

    // ts field is a unix timestamp
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("ts", Schema.INT64_SCHEMA)
            .field("other", Schema.STRING_SCHEMA)
            .build();
    Struct original = new Struct(structWithTimestampFieldSchema);
    original.put("ts", DATE_PLUS_TIME_UNIX);
    original.put("other", "test");

    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.toString(), header.schema().toString());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testWithSchemaFieldConversion_Micros() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "microseconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);

    // ts field is a unix timestamp with microseconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build();
    Struct original = new Struct(structWithTimestampFieldSchema);
    original.put("ts", DATE_PLUS_TIME_UNIX_MICROS);

    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testWithSchemaFieldConversion_Nanos() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "nanoseconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);

    // ts field is a unix timestamp with microseconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build();
    Struct original = new Struct(structWithTimestampFieldSchema);
    original.put("ts", DATE_PLUS_TIME_UNIX_NANOS);

    SourceRecord transformed =
        transformer.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testWithSchemaFieldConversion_Seconds() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build();
    Struct original = new Struct(structWithTimestampFieldSchema);
    original.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial = createRecordWithSchema(structWithTimestampFieldSchema, original);
    final SourceRecord transformed = transformer.apply(initial);

    Calendar expectedDate = GregorianCalendar.getInstance(UTC);
    expectedDate.setTimeInMillis(0L);
    expectedDate.add(Calendar.DATE, 1);
    expectedDate.add(Calendar.SECOND, 1);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(expectedDate.getTime(), header.value());
  }

  @Test
  void testWithSchemaValuePrefixedFieldConversion_Seconds() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_value.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build();
    Struct original = new Struct(structWithTimestampFieldSchema);
    original.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial = createRecordWithSchema(structWithTimestampFieldSchema, original);
    final SourceRecord transformed = transformer.apply(initial);

    Calendar expectedDate = GregorianCalendar.getInstance(UTC);
    expectedDate.setTimeInMillis(0L);
    expectedDate.add(Calendar.DATE, 1);
    expectedDate.add(Calendar.SECOND, 1);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(expectedDate.getTime(), header.value());
  }

  @Test
  void testWithSchemaKeyPrefixedFieldConversion_Seconds() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_key.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build();
    Struct original = new Struct(structWithTimestampFieldSchema);
    original.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, original, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Calendar expectedDate = GregorianCalendar.getInstance(UTC);
    expectedDate.setTimeInMillis(0L);
    expectedDate.add(Calendar.DATE, 1);
    expectedDate.add(Calendar.SECOND, 1);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(expectedDate.getTime(), header.value());
  }

  @Test
  void testWithSchemaNestedFieldConversion_Seconds() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "level1.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build());
    tsStruct.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial = createRecordWithSchema(structWithTimestampFieldSchema, rootStruct);
    final SourceRecord transformed = transformer.apply(initial);

    Calendar expectedDate = GregorianCalendar.getInstance(UTC);
    expectedDate.setTimeInMillis(0L);
    expectedDate.add(Calendar.DATE, 1);
    expectedDate.add(Calendar.SECOND, 1);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(expectedDate.getTime(), header.value());
  }

  @Test
  void testWithSchemaNestedKeyFieldConversion_Seconds() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_key.level1.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build());
    tsStruct.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, rootStruct, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Calendar expectedDate = GregorianCalendar.getInstance(UTC);
    expectedDate.setTimeInMillis(0L);
    expectedDate.add(Calendar.DATE, 1);
    expectedDate.add(Calendar.SECOND, 1);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(expectedDate.getTime(), header.value());
  }

  @Test
  void testSchemalessStringToUnix_Micros() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "microseconds");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Schema.INT64_SCHEMA.toString(), header.schema().toString());
    assertEquals(86401234000L, header.value());
  }

  @Test
  void testSchemalessStringToUnix_Nanos() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "nanoseconds");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Schema.INT64_SCHEMA.type(), header.schema().type());
    assertEquals(86401234000000L, header.value());
  }

  @Test
  void testSchemalessStringToUnix_Seconds() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed = transformer.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Schema.INT64_SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME_UNIX_SECONDS, header.value());
  }

  // Validate Key implementation in addition to Value

  @Test
  void testKey() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_key");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    SourceRecord transformed =
        transformer.apply(
            new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime(), null, null));

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    assertEquals(DATE_PLUS_TIME.getTime(), header.value());
  }

  @Test
  void testWithSchemaNestedKeyFieldConversion15SecondsWindow() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_key.level1.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    config.put(TimestampConverter.ROLLING_WINDOW_TYPE_CONFIG, "seconds");
    config.put(TimestampConverter.ROLLING_WINDOW_SIZE_CONFIG, "15");

    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build());

    Instant start = Instant.parse("2018-01-01T12:12:12.122Z");

    tsStruct.put("ts", start.getEpochSecond());
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, rootStruct, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    final Instant expected = Instant.parse("2018-01-01T12:12:00.000Z");
    final java.util.Date expectedDate = java.util.Date.from(expected);
    assertEquals(expectedDate, header.value());
  }

  @Test
  void testWithSchemaNestedKeyFieldConversion2HoursTimestampWindow() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_key.level1.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    config.put(TimestampConverter.ROLLING_WINDOW_TYPE_CONFIG, "hours");
    config.put(TimestampConverter.ROLLING_WINDOW_SIZE_CONFIG, "2");

    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build());

    Instant start = Instant.parse("2018-01-01T13:12:12.122Z");

    tsStruct.put("ts", start.getEpochSecond());
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, rootStruct, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    final Instant expected = Instant.parse("2018-01-01T12:00:00.000Z");
    final java.util.Date expectedDate = java.util.Date.from(expected);
    assertEquals(expectedDate, header.value());
  }

  @Test
  void testWithSchemaNestedKeyFieldConversion2HoursStringWindow() {

    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FORMAT_TO_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FIELD_CONFIG, "_key.level1.ts");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    config.put(TimestampConverter.ROLLING_WINDOW_TYPE_CONFIG, "hours");
    config.put(TimestampConverter.ROLLING_WINDOW_SIZE_CONFIG, "2");

    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.STRING_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.STRING_SCHEMA).build());

    Instant start = Instant.parse("2018-01-01T13:12:12.122Z");
    String originalString =
        DateTimeFormatter.ofPattern(STRING_DATE_FMT).withZone(ZoneOffset.UTC).format(start);

    tsStruct.put("ts", originalString);
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, rootStruct, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Schema.Type.STRING, header.schema().type());

    assertEquals("2018 01 01 12 00 00 000 UTC", header.value());
  }

  @Test
  void testWithSchemaNestedKeyFieldConversion2HoursStringWindowWhenSourceIsString() {

    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.FORMAT_FROM_CONFIG, STRING_DATE_FMT);
    config.put(TimestampConverter.FORMAT_TO_CONFIG, "yyyy-MM-dd HH:mm");
    config.put(TimestampConverter.FIELD_CONFIG, "_key.level1.ts");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    config.put(TimestampConverter.ROLLING_WINDOW_TYPE_CONFIG, "hours");
    config.put(TimestampConverter.ROLLING_WINDOW_SIZE_CONFIG, "2");

    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.STRING_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.STRING_SCHEMA).build());

    Instant start = Instant.parse("2018-01-01T13:12:12.122Z");
    String originalString =
        DateTimeFormatter.ofPattern(STRING_DATE_FMT).withZone(ZoneOffset.UTC).format(start);

    tsStruct.put("ts", originalString);
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, rootStruct, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Schema.Type.STRING, header.schema().type());

    assertEquals("2018-01-01 12:00", header.value());
  }

  @Test
  void testWithSchemaNestedKeyFieldConversion10MinutesWindow() {
    Map<String, String> config = new HashMap<>();
    config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
    config.put(TimestampConverter.FIELD_CONFIG, "_key.level1.ts");
    config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
    config.put(TimestampConverter.HEADER_NAME_CONFIG, "ts_header");
    config.put(TimestampConverter.ROLLING_WINDOW_TYPE_CONFIG, "minutes");
    config.put(TimestampConverter.ROLLING_WINDOW_SIZE_CONFIG, "10");

    // ts field is a unix timestamp with seconds precision
    Schema structWithTimestampFieldSchema =
        SchemaBuilder.struct()
            .field("level1", SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build())
            .build();
    Struct rootStruct = new Struct(structWithTimestampFieldSchema);
    Struct tsStruct = new Struct(SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).build());

    // Get epoch for 2018-01-01T12:12:12.122Z
    Instant start = Instant.parse("2018-01-01T12:12:12.122Z");

    tsStruct.put("ts", start.getEpochSecond());
    rootStruct.put("level1", tsStruct);

    final TimestampConverter<SourceRecord> transformer = new TimestampConverter<>();
    transformer.configure(config);
    final SourceRecord initial =
        new SourceRecord(
            null, null, "topic", 0, structWithTimestampFieldSchema, rootStruct, null, null);
    final SourceRecord transformed = transformer.apply(initial);

    Header header = transformed.headers().lastWithName("ts_header");
    assertNotNull(header);
    assertEquals(Timestamp.SCHEMA.type(), header.schema().type());
    final Instant expected = Instant.parse("2018-01-01T12:10:00.000Z");
    final java.util.Date expectedDate = java.util.Date.from(expected);
    assertEquals(expectedDate, header.value());
  }

  private SourceRecord createRecordWithSchema(Schema schema, Object value) {
    return new SourceRecord(null, null, "topic", 0, schema, value);
  }

  private SourceRecord createRecordSchemaless(Object value) {
    return createRecordWithSchema(null, value);
  }
}
