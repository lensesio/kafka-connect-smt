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

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RecordFieldTimestamp} unix.precision configuration tests are not required since
 * Connect covers that
 */
class RecordFieldTimestampTest {

  @Test
  void nullRecordReturnsNullInstant() {
    // generate the test creating an instance of RecordFieldTimestamp
    Map<String, Object> props = new HashMap<>();
    props.put("field", "value");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    Instant actual =
        recordFieldTimestamp.getInstant(new SinkRecord("topic", 0, null, null, null, null, 0));
    assert actual == null;
  }

  @Test
  void fieldKeySetsTheFieldTypeToKeyTest() {
    // generate the test creating an instance of RecordFieldTimestamp
    String[] fields = new String[] {"_key", "_key.a", "_key.a.b"};
    Arrays.stream(fields)
        .forEach(
            field -> {
              Map<String, Object> props = new HashMap<>();
              props.put("field", field);
              props.put("format.from.pattern", "yyyy-MM-dd");
              props.put("unix.precision", "milliseconds");

              SimpleConfig config =
                  new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
              RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
                  RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
              assertEquals(
                  recordFieldTimestamp.getFieldTypeAndFields().getFieldType(), FieldType.KEY);
            });
  }

  @Test
  void unixPrecisionDefaultsWhenNotSet() {
    // generate the test creating an instance of RecordFieldTimestamp
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_key");
    props.put("format.from.pattern", "yyyy-MM-dd");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    assertEquals(recordFieldTimestamp.getUnixPrecision(), "milliseconds");
  }

  @Test
  void fieldKeySetsTheFieldTypeToValueTest() {
    String[] fields = new String[] {"_value", "a", "a.b"};
    Arrays.stream(fields)
        .forEach(
            field -> {
              Map<String, Object> props = new HashMap<>();
              props.put("field", field);
              props.put("format.from.pattern", "yyyy-MM-dd");
              props.put("unix.precision", "milliseconds");

              SimpleConfig config =
                  new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
              RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
                  RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
              assertEquals(
                  recordFieldTimestamp.getFieldTypeAndFields().getFieldType(), FieldType.VALUE);
            });
  }

  @Test
  void fieldTimestampSetTheFieldTypeToTimestamp() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_timestamp");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    assertEquals(recordFieldTimestamp.getFieldTypeAndFields().getFieldType(), FieldType.TIMESTAMP);
  }

  @Test
  void extendConfigDef() {
    ConfigDef configDef = RecordFieldTimestamp.extendConfigDef(new ConfigDef());
    assert configDef.configKeys().size() == 3;
    assert configDef.configKeys().containsKey("field");
    assert configDef.configKeys().containsKey("format.from.pattern");
    assert configDef.configKeys().containsKey("unix.precision");
  }

  @Test
  void whenFieldIsSetToTimestampTheRecordTimestampIsReturned() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_timestamp");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedTs = 123456789L;
    Instant actual =
        recordFieldTimestamp.getInstant(
            new SinkRecord(
                "topic", 0, null, null, null, null, 0, expectedTs, TimestampType.LOG_APPEND_TIME));
    assertEquals(actual.toEpochMilli(), expectedTs);
  }

  @Test
  void whenFieldIsSetToKeyItReturnsTheRecordKeyValueTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_key");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedKey = 123456789L;

    SinkRecord[] records =
        new SinkRecord[] {
          new SinkRecord("topic", 0, null, expectedKey, null, null, 0),
          new SinkRecord(
              "topic",
              0,
              null,
              expectedKey,
              Schema.INT64_SCHEMA,
              null,
              0,
              0L,
              TimestampType.LOG_APPEND_TIME)
        };
    Arrays.stream(records)
        .forEach(
            record -> {
              Instant actual = recordFieldTimestamp.getInstant(record);
              assertEquals(actual.toEpochMilli(), expectedKey);
            });
  }

  @Test
  void whenFieldIsSetToValueItReturnsTheRecordValueTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_value");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 123456789L;

    SinkRecord[] records =
        new SinkRecord[] {
          new SinkRecord("topic", 0, null, null, null, expectedValue, 0),
          new SinkRecord(
              "topic",
              0,
              null,
              null,
              Schema.INT64_SCHEMA,
              expectedValue,
              0,
              0L,
              TimestampType.LOG_APPEND_TIME)
        };
    Arrays.stream(records)
        .forEach(
            record -> {
              Instant actual = recordFieldTimestamp.getInstant(record);
              assertEquals(actual.toEpochMilli(), expectedValue);
            });
  }

  @Test
  void returnsTheKeyStructFieldWhenTheValueIsaLongTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_key.a");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 123456789L;

    Schema schema =
        SchemaBuilder.struct()
            .field("a", Schema.INT64_SCHEMA)
            .field("b", Schema.STRING_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("a", expectedValue).put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, schema, struct, null, expectedValue, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }

  @Test
  void returnsTheValueStructFieldWhenTheValueIsALongTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "a");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 123456789L;

    Schema schema =
        SchemaBuilder.struct()
            .field("a", Schema.INT64_SCHEMA)
            .field("b", Schema.STRING_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("a", expectedValue).put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, null, null, schema, struct, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }

  @Test
  void returnsAKeyFieldWhenTheFieldValueIsAStringDateTimeRepresentationTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_key.a");
    props.put("format.from.pattern", "yyyy-MM-dd HH:mm:ss");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 1704067260000L;
    String dateTimeValue = "2024-01-01 00:01:00";

    Schema schema =
        SchemaBuilder.struct()
            .field("a", Schema.STRING_SCHEMA)
            .field("b", Schema.STRING_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("a", dateTimeValue).put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, schema, struct, null, expectedValue, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }

  // create the tests where instead of struct we have a Map and schema is null

  @Test
  void returnsTheKeyMapFieldWhenTheValueIsaLongTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_key.a");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 123456789L;

    Map<String, Object> map = new HashMap<>();
    map.put("a", expectedValue);
    map.put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, null, map, null, expectedValue, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }

  @Test
  void returnsTheValueMapFieldWhenTheValueIsALongTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "a");
    props.put("format.from.pattern", "yyyy-MM-dd");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 123456789L;

    Map<String, Object> map = new HashMap<>();
    map.put("a", expectedValue);
    map.put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, null, null, null, map, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }

  @Test
  void returnsTheValueMapFieldWhenTheValueIsADateTimeStringTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "a");
    props.put("format.from.pattern", "yyyy-MM-dd HH:mm:ss");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 1704067260000L;
    String dateTimeValue = "2024-01-01 00:01:00";

    Map<String, Object> map = new HashMap<>();
    map.put("a", dateTimeValue);
    map.put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, null, null, null, map, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }

  @Test
  void returnsTheKeyMapFieldWhichContainsTheDateTimeStringTest() {
    Map<String, Object> props = new HashMap<>();
    props.put("field", "_key.a");
    props.put("format.from.pattern", "yyyy-MM-dd HH:mm:ss");
    props.put("unix.precision", "milliseconds");

    SimpleConfig config =
        new SimpleConfig(RecordFieldTimestamp.extendConfigDef(new ConfigDef()), props);
    RecordFieldTimestamp<SinkRecord> recordFieldTimestamp =
        RecordFieldTimestamp.create(config, UTC, Locale.getDefault());
    long expectedValue = 1704067260000L;
    String dateTimeValue = "2024-01-01 00:01:00";

    Map<String, Object> map = new HashMap<>();
    map.put("a", dateTimeValue);
    map.put("b", "value");

    SinkRecord input = new SinkRecord("topic", 0, null, map, null, expectedValue, 0);

    Instant actual = recordFieldTimestamp.getInstant(input);
    assertEquals(actual.toEpochMilli(), expectedValue);
  }
}
