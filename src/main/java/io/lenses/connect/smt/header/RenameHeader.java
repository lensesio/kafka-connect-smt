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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/** A Kafka Connect SMT that renames a list of headers. */
public class RenameHeader<R extends ConnectRecord<R>> implements Transformation<R> {

  private String oldHeaderKey;
  private String newHeaderKey;

  private interface ConfigName {
    String HEADER_OLD_KEY = "header.name.old";
    String HEADER_NEW_KEY = "header.name.new";
  }

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ConfigName.HEADER_OLD_KEY,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "The old header name.")
          .define(
              ConfigName.HEADER_NEW_KEY,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "The new header name.");

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    oldHeaderKey = validateConfig(config, ConfigName.HEADER_OLD_KEY);
    newHeaderKey = validateConfig(config, ConfigName.HEADER_NEW_KEY);
  }

  private String validateConfig(SimpleConfig config, String configName) {
    String value = config.getString(configName);
    if (value == null) {
      throw new ConfigException("Configuration '" + configName + "' must be set.");
    } else if (value.isEmpty()) {
      throw new ConfigException("Configuration '" + configName + "' must not be empty.");
    }
    return value;
  }

  @Override
  public R apply(R r) {
    if (r == null) {
      return null;
    }
    final Iterator<Header> allHeaders = r.headers().allWithName(oldHeaderKey);
    final List<Header> headers = new ArrayList<>();
    allHeaders.forEachRemaining(headers::add);

    r.headers().remove(oldHeaderKey);
    headers.forEach(header -> r.headers().add(newHeaderKey, header.value(), header.schema()));
    return r;
  }
}
