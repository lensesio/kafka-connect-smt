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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class RenameHeaderTest {

  @Test
  void testRenameHeader() {
    RenameHeader<SinkRecord> renameHeader = new RenameHeader<>();
    renameHeader.configure(Map.of("header.name.old", "oldHeader", "header.name.new", "newHeader"));

    final SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            123456789L,
            Schema.INT64_SCHEMA,
            null,
            0,
            0L,
            TimestampType.LOG_APPEND_TIME);
    sinkRecord.headers().addString("oldHeader", "value");
    sinkRecord.headers().addString("anotherHeader", "value");

    final SinkRecord transformedRecord = renameHeader.apply(sinkRecord);
    assertEquals("value", transformedRecord.headers().lastWithName("newHeader").value());
    assertEquals("value", transformedRecord.headers().lastWithName("anotherHeader").value());
    assertNull(transformedRecord.headers().lastWithName("oldHeader"));
  }

  @Test
  void handlesNullRecord() {
    RenameHeader<SinkRecord> renameHeader = new RenameHeader<>();
    renameHeader.configure(Map.of("header.name.old", "oldHeader", "header.name.new", "newHeader"));

    final SinkRecord transformedRecord = renameHeader.apply(null);
    assertNull(transformedRecord);
  }

  @Test
  void handlesMissingHeader() {
    RenameHeader<SinkRecord> renameHeader = new RenameHeader<>();
    renameHeader.configure(Map.of("header.name.old", "oldHeader", "header.name.new", "newHeader"));

    final SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            123456789L,
            Schema.INT64_SCHEMA,
            null,
            0,
            0L,
            TimestampType.LOG_APPEND_TIME);
    sinkRecord.headers().addString("anotherHeader", "value");

    final SinkRecord transformedRecord = renameHeader.apply(sinkRecord);
    assertEquals("value", transformedRecord.headers().lastWithName("anotherHeader").value());
    assertNull(transformedRecord.headers().lastWithName("oldHeader"));
    assertNull(transformedRecord.headers().lastWithName("newHeader"));
  }

  @Test
  void keepsTheHeaderValue() {
    RenameHeader<SinkRecord> renameHeader = new RenameHeader<>();
    renameHeader.configure(Map.of("header.name.old", "oldHeader", "header.name.new", "newHeader"));

    final SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            123456789L,
            Schema.INT64_SCHEMA,
            null,
            0,
            0L,
            TimestampType.LOG_APPEND_TIME);
    sinkRecord.headers().addBoolean("oldHeader", true);

    final SinkRecord transformedRecord = renameHeader.apply(sinkRecord);
    assertEquals(true, transformedRecord.headers().lastWithName("newHeader").value());
  }

  @Test
  void renamesAllHeadersWithTheSameName() {
    RenameHeader<SinkRecord> renameHeader = new RenameHeader<>();
    renameHeader.configure(Map.of("header.name.old", "oldHeader", "header.name.new", "newHeader"));

    final SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            123456789L,
            Schema.INT64_SCHEMA,
            null,
            0,
            0L,
            TimestampType.LOG_APPEND_TIME);
    sinkRecord.headers().addString("oldHeader", "value1");
    sinkRecord.headers().addString("oldHeader", "value2");

    final SinkRecord transformedRecord = renameHeader.apply(sinkRecord);

    final HashSet<Object> newHeaderValues = new HashSet<>();
    transformedRecord
        .headers()
        .allWithName("newHeader")
        .forEachRemaining(header -> newHeaderValues.add(header.value()));

    assertEquals(Set.of("value1", "value2"), newHeaderValues);
    assertNull(transformedRecord.headers().lastWithName("oldHeader"));
  }
}
