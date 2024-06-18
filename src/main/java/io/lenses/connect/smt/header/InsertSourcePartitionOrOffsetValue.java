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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class InsertSourcePartitionOrOffsetValue implements Transformation<SourceRecord> {

  public static final String DEFAULT_PREFIX_OFFSET = "offset.";
  public static final String DEFAULT_PREFIX_PARTITION = "partition..";
  Configuration offsetConfig;
  Configuration partitionConfig;

  static class Configuration {

    private final List<String> fields;
    private final String prefix;

    public Configuration(final List<String> fields, final String prefix) {
      this.fields = fields;
      this.prefix = prefix;
    }

    public List<String> getFields() {
      return fields;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  private static final String KEY_PARTITION_FIELDS = "partition.fields";
  private static final String KEY_PARTITION_PREFIX = "partition.prefix";

  private static final String KEY_OFFSET_FIELDS = "offset.fields";
  private static final String KEY_OFFSET_PREFIX = "offset.prefix";

  @Override
  public SourceRecord apply(SourceRecord sourceRecord) {
    addHeadersFromConfig(offsetConfig, sourceRecord, sourceRecord.sourceOffset());
    addHeadersFromConfig(partitionConfig, sourceRecord, sourceRecord.sourcePartition());
    return sourceRecord;
  }

  private void addHeadersFromConfig(
      Configuration offsetConfig, SourceRecord sourceRecord, Map<String, ?> partitionOrOffsetMap) {
    offsetConfig
        .getFields()
        .forEach(
            f ->
                sourceRecord
                    .headers()
                    .addString(offsetConfig.getPrefix() + f, (String) partitionOrOffsetMap.get(f)));
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            KEY_OFFSET_FIELDS,
            ConfigDef.Type.LIST,
            Collections.emptyList(),
            ConfigDef.Importance.HIGH,
            "Comma-separated list of fields to retrieve from the offset")
        .define(
            KEY_OFFSET_PREFIX,
            ConfigDef.Type.STRING,
            DEFAULT_PREFIX_OFFSET,
            ConfigDef.Importance.LOW,
            "Optional prefix for offset keys")
        .define(
            KEY_PARTITION_FIELDS,
            ConfigDef.Type.LIST,
            Collections.emptyList(),
            ConfigDef.Importance.HIGH,
            "Comma-separated list of fields to retrieve from the partition")
        .define(
            KEY_PARTITION_PREFIX,
            ConfigDef.Type.STRING,
            DEFAULT_PREFIX_PARTITION,
            ConfigDef.Importance.LOW,
            "Optional prefix for partition keys");
  }

  @Override
  public void close() {
    // nothing to close
  }

  @Override
  public void configure(Map<String, ?> map) {
    offsetConfig =
        new Configuration(getFields(map, KEY_OFFSET_FIELDS), getPrefix(map, KEY_OFFSET_PREFIX));
    partitionConfig =
        new Configuration(
            getFields(map, KEY_PARTITION_FIELDS), getPrefix(map, KEY_PARTITION_PREFIX));
  }

  private static String getPrefix(Map<String, ?> map, String keyOffsetPrefix) {
    return Optional.ofNullable((String) map.get(keyOffsetPrefix))
        // this exception should never be thrown if Kafka Connect assures the default value
        .orElseThrow(() -> new IllegalStateException(keyOffsetPrefix + "not specified"));
  }

  private List<String> getFields(Map<String, ?> map, String offsetFields) {
    return Optional.ofNullable(map.get(offsetFields)).stream()
        .map(
            p -> {
              if (!(p instanceof List)) {
                throw new IllegalStateException(offsetFields + " should be a List");
              }
              return ((List<?>) p);
            })
        .flatMap(p -> p.stream().map(Object::toString))
        .collect(Collectors.toList());
  }
}
