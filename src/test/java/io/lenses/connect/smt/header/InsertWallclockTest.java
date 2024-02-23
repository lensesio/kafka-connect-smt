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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link io.lenses.connect.smt.header.InsertWallclockDateTimePart}. */
public class InsertWallclockTest {
  @Test
  public void testApplyWithEpochValue() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "epoch");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    transformer.configure(configs);
    final Instant instant = Instant.now();
    transformer.setInstantF(() -> instant);

    // Create a source record
    Headers headers = new ConnectHeaders();
    SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "value",
            null,
            null,
            System.currentTimeMillis(),
            headers);

    SourceRecord transformedRecord = transformer.apply(record);

    Headers transformedHeaders = transformedRecord.headers();
    assertEquals(1, transformedHeaders.size());
    assertEquals(transformedHeaders.iterator().next().key(), "wallclock");
    assertEquals(
        transformedHeaders.iterator().next().value(), String.valueOf(instant.toEpochMilli()));
  }

  @Test
  public void testRaiseConfigExceptionIfTimezoneIsNotUTCAndEpochModeIsSet() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "epoch");
    configs.put("timezone", "Europe/Paris");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    try {
      transformer.configure(configs);
      fail("It should have raised a ConfigException");
    } catch (Exception e) {
      assertEquals(
          e.getMessage(),
          "Configuration 'timezone' must be set to 'UTC' when 'value.type' is set to 'epoch'.");
    }
  }

  @Test
  public void testApplyWithStringValue() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "format");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    transformer.configure(configs);
    final String dateTime = "2020-01-01T00:00:00.000Z";
    final Instant instant = Instant.parse(dateTime);
    transformer.setInstantF(() -> instant);

    // Create a source record
    Headers headers = new ConnectHeaders();
    SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "value",
            null,
            null,
            System.currentTimeMillis(),
            headers);

    // Apply the transformation
    SourceRecord transformedRecord = transformer.apply(record);

    // Verify the header is inserted
    Headers transformedHeaders = transformedRecord.headers();
    assertEquals(1, transformedHeaders.size());
    assertEquals(transformedHeaders.iterator().next().key(), "wallclock");
    assertEquals(transformedHeaders.iterator().next().value(), "2020-01-01T00:00:00");
  }

  @Test
  public void testApplyWithStringValueAndCustomFormatter() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "format");
    configs.put("format", "yyyy-MM-dd HH:mm:ss");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    transformer.configure(configs);
    final Instant instant = Instant.parse("2020-01-01T00:00:00.000Z");
    transformer.setInstantF(() -> instant);

    // Create a source record
    Headers headers = new ConnectHeaders();
    SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "value",
            null,
            null,
            System.currentTimeMillis(),
            headers);

    // Apply the transformation
    SourceRecord transformedRecord = transformer.apply(record);

    // Verify the header is inserted
    Headers transformedHeaders = transformedRecord.headers();
    assertEquals(1, transformedHeaders.size());
    assertEquals(transformedHeaders.iterator().next().key(), "wallclock");
    assertEquals(transformedHeaders.iterator().next().value(), "2020-01-01 00:00:00");
  }

  @Test
  public void testApplyWithStringValueAndCustomFormatterAndTimezoneIsKolkata() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "format");
    configs.put("format", "yyyy-MM-dd HH:mm:ss");
    configs.put("timezone", "Asia/Kolkata");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    transformer.configure(configs);
    final Instant instant = Instant.parse("2020-01-01T00:00:00.000Z");
    transformer.setInstantF(() -> instant);

    // Create a source record
    Headers headers = new ConnectHeaders();
    SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "value",
            null,
            null,
            System.currentTimeMillis(),
            headers);

    // Apply the transformation
    SourceRecord transformedRecord = transformer.apply(record);

    // Verify the header is inserted
    Headers transformedHeaders = transformedRecord.headers();
    assertEquals(1, transformedHeaders.size());
    assertEquals(transformedHeaders.iterator().next().key(), "wallclock");
    assertEquals(transformedHeaders.iterator().next().value(), "2020-01-01 05:30:00");
  }

  @Test
  public void testIgnoreFormatIfEpochModeIsSet() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "epoch");
    configs.put("format", "yyyy-MM-dd HH:mm:ss");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    transformer.configure(configs);
    final Instant instant = Instant.parse("2020-01-01T00:00:00.000Z");
    transformer.setInstantF(() -> instant);

    // Create a source record
    Headers headers = new ConnectHeaders();
    SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "value",
            null,
            null,
            System.currentTimeMillis(),
            headers);

    // Apply the transformation
    SourceRecord transformedRecord = transformer.apply(record);

    // Verify the header is inserted
    Headers transformedHeaders = transformedRecord.headers();
    assertEquals(1, transformedHeaders.size());
    assertEquals(transformedHeaders.iterator().next().key(), "wallclock");
    assertEquals(
        transformedHeaders.iterator().next().value(), String.valueOf(instant.toEpochMilli()));
  }

  @Test
  public void testInvalidValueTypeRaisesConfigException() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "invalid");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    try {
      transformer.configure(configs);

      fail("It should have raised a ConfigException");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "'value.type' must be set to either 'epoch' or 'string'");
    }
  }

  @Test
  public void testInvalidFormatRaisesConfigException() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    configs.put("value.type", "format");
    configs.put("format", "invalid");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    try {
      transformer.configure(configs);
      fail("It should have raised a ConfigException");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Configuration 'format' is not a valid date format.");
    }
  }

  @Test
  public void testValueTypeNotSetDefaultsToFormat() {
    Map<String, String> configs = new HashMap<>();
    configs.put("header.name", "wallclock");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    transformer.configure(configs);
    final String dateTime = "2020-01-01T00:00:00.000Z";
    final Instant instant = Instant.parse(dateTime);
    transformer.setInstantF(() -> instant);

    // Create a source record
    Headers headers = new ConnectHeaders();
    SourceRecord record =
        new SourceRecord(
            null,
            null,
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "value",
            null,
            null,
            System.currentTimeMillis(),
            headers);

    // Apply the transformation
    SourceRecord transformedRecord = transformer.apply(record);

    // Verify the header is inserted
    Headers transformedHeaders = transformedRecord.headers();
    assertEquals(1, transformedHeaders.size());
    assertEquals(transformedHeaders.iterator().next().key(), "wallclock");
    assertEquals(transformedHeaders.iterator().next().value(), "2020-01-01T00:00:00");
  }

  @Test
  public void testMissingHeaderNameRaisesConfigException() {
    Map<String, String> configs = new HashMap<>();
    configs.put("value.type", "format");
    InsertWallclock<SourceRecord> transformer = new InsertWallclock<>();
    try {
      transformer.configure(configs);
      fail("It should have raised a ConfigException");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Configuration 'header.name' must be set.");
    }
  }
}
