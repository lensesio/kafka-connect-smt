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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

class PropsFormatterTest {

  @Test
  void singleEntry() {
    Map<String, Object> props = Map.of("something", "else");
    PropsFormatter writer = new PropsFormatter(new SimpleConfig(new ConfigDef(), props));
    assertEquals("{something: \"else\"}", writer.apply());
  }

  @Test
  void multipleEntries() {
    Map<String, Object> props = Map.of("first", "item", "something", "else");
    PropsFormatter writer = new PropsFormatter(new SimpleConfig(new ConfigDef(), props));
    assertEquals("{first: \"item\", something: \"else\"}", writer.apply());
  }
}
