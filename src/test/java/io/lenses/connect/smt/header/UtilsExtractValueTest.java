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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

public class UtilsExtractValueTest {

  @Test
  public void extractValueReturnsCorrectValueFromMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    Object result = Utils.extractValue(map, new String[] {"key"});
    assertEquals("value", result);
  }

  @Test
  public void extractValueReturnsNullWhenFieldDoesNotExistInMap() {
    Map<String, Object> map = new HashMap<>();
    Object result = Utils.extractValue(map, new String[] {"nonexistent"});
    assertNull(result);
  }

  @Test
  public void extractValueReturnsCorrectValueFromNestedMap() {
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("key", "value");
    Map<String, Object> map = new HashMap<>();
    map.put("nested", nestedMap);
    Object result = Utils.extractValue(map, new String[] {"nested", "key"});
    assertEquals("value", result);
  }

  @Test
  public void extractValueReturnsNullWhenFieldDoesNotExistInNestedMap() {
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("key", "value");
    Map<String, Object> map = new HashMap<>();
    map.put("nested", nestedMap);
    Object result = Utils.extractValue(map, new String[] {"nested", "nonexistent"});
    assertNull(result);
  }

  @Test
  public void extractValueFromAKafkaConnectStruct() {
    Schema schema =
        SchemaBuilder.struct()
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.INT32_SCHEMA)
            .build();

    Struct struct = new Struct(schema).put("field1", "value1").put("field2", 42);

    Object result = Utils.extractValue(struct, new String[] {"field1"});
    assertEquals("value1", result);
  }

  @Test
  public void extractValueReturnsNullWhenFieldDoesNotExistInStruct() {
    Schema schema =
        SchemaBuilder.struct()
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.INT32_SCHEMA)
            .build();

    Struct struct = new Struct(schema).put("field1", "value1").put("field2", 42);

    Object result = Utils.extractValue(struct, new String[] {"nonexistent"});
    assertNull(result);
  }

  @Test
  public void extractValueReturnsCorrectValueFromNestedStruct() {
    Schema nestedSchema =
        SchemaBuilder.struct()
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.INT32_SCHEMA)
            .build();

    Schema schema = SchemaBuilder.struct().field("nested", nestedSchema).build();

    Struct nestedStruct = new Struct(nestedSchema).put("field1", "value1").put("field2", 42);

    Struct struct = new Struct(schema).put("nested", nestedStruct);

    Object result = Utils.extractValue(struct, new String[] {"nested", "field1"});
    assertEquals("value1", result);
  }
}
