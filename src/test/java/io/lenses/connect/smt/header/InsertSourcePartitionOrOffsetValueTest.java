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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InsertSourcePartitionOrOffsetValueTest {

  private InsertSourcePartitionOrOffsetValue transform;

  @BeforeEach
  public void setUp() {
    transform = new InsertSourcePartitionOrOffsetValue();
  }

  @Test
  void testApplyAddsOffsetHeaders() {
    Map<String, String> sourceOffset =
        Map.of(
            "field1", "value1",
            "field2", "value2");

    SourceRecord record =
        new SourceRecord(
            null,
            sourceOffset,
            "test-topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "value",
            0L,
            new ConnectHeaders());

    Map<String, Object> config =
        Map.of(
            "offset.fields",
            List.of("field1", "field2"),
            "offset.prefix",
            "offset.",
            "partition.fields",
            Collections.emptyList(),
            "partition.prefix",
            "");

    transform.configure(config);

    SourceRecord transformedRecord = transform.apply(record);

    assertEquals("value1", transformedRecord.headers().lastWithName("offset.field1").value());
    assertEquals("value2", transformedRecord.headers().lastWithName("offset.field2").value());
  }

  @Test
  void testApplyAddsPartitionHeaders() {
    Map<String, String> sourcePartition =
        Map.of(
            "partField1", "partValue1",
            "partField2", "partValue2");

    SourceRecord record =
        new SourceRecord(
            sourcePartition,
            null,
            "test-topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "value",
            0L,
            new ConnectHeaders());

    Map<String, Object> config =
        Map.of(
            "partition.fields",
            List.of("partField1", "partField2"),
            "partition.prefix",
            "partition..",
            "offset.fields",
            Collections.emptyList(),
            "offset.prefix",
            "");

    transform.configure(config);

    SourceRecord transformedRecord = transform.apply(record);

    assertEquals(
        "partValue1", transformedRecord.headers().lastWithName("partition..partField1").value());
    assertEquals(
        "partValue2", transformedRecord.headers().lastWithName("partition..partField2").value());
  }

  @Test
  void testConfigure() {
    Map<String, Object> config =
        Map.of(
            "offset.fields",
            List.of("field1", "field2"),
            "offset.prefix",
            "offset.",
            "partition.fields",
            List.of("partField1", "partField2"),
            "partition.prefix",
            "partition..");

    transform.configure(config);

    assertEquals(List.of("field1", "field2"), transform.offsetConfig.getFields());
    assertEquals("offset.", transform.offsetConfig.getPrefix());
    assertEquals(List.of("partField1", "partField2"), transform.partitionConfig.getFields());
    assertEquals("partition..", transform.partitionConfig.getPrefix());
  }

  @Test
  void testClose() {
    try (InsertSourcePartitionOrOffsetValue transform = new InsertSourcePartitionOrOffsetValue()) {

      // Since the close method does nothing, we simply ensure it runs without errors
      assertDoesNotThrow(transform::close);
    }
  }
}
