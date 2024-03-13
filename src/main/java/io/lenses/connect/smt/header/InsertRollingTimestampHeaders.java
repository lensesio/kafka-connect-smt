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

import java.time.Instant;
import java.util.Collections;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformer which takes the record timestamp and inserts a header year, month, day, hour,
 * minute, second and day. The benefit over the InsertField SMT is that the payload is not modified
 * leading to less memory used and less CPU time.
 *
 * @param <R> the record type
 */
abstract class InsertRollingTimestampHeaders<R extends ConnectRecord<R>>
    extends InsertTimestampHeaders<R> {

  public static ConfigDef CONFIG_DEF =
      InsertTimestampHeaders.CONFIG_DEF
          .define(
              ConfigName.ROLLING_WINDOW_SIZE_CONFIG,
              ConfigDef.Type.INT,
              ConfigName.DEFAULT_ROLLING_WINDOW_VALUE,
              ConfigDef.Importance.HIGH,
              "The rolling window size. For example, if the rolling window is set to 'minutes' "
                  + "and the rolling window value is set to 15, then the rolling window "
                  + "is 15 minutes.")
          .define(
              ConfigName.ROLLING_WINDOW_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_ROLLING_WINDOW.name(),
              ConfigDef.Importance.HIGH,
              "The rolling window type. The allowed values are hours, minutes or seconds.");
  private RollingWindowDetails rollingWindowDetails;

  public InsertRollingTimestampHeaders() {
    super(CONFIG_DEF);
  }

  interface ConfigName {
    String ROLLING_WINDOW_TYPE_CONFIG = "rolling.window.type";
    String ROLLING_WINDOW_SIZE_CONFIG = "rolling.window.size";

    int DEFAULT_ROLLING_WINDOW_VALUE = 15;
    RollingWindow DEFAULT_ROLLING_WINDOW = RollingWindow.MINUTES;
  }

  protected abstract Instant getInstantInternal(R r);

  @Override
  protected Instant getInstant(R r) {
    return rollingWindowDetails.adjust(getInstantInternal(r));
  }

  @Override
  protected void configureInternal(SimpleConfig config) {
    final RollingWindow rollingWindow =
        RollingWindowUtils.extractRollingWindow(
                config, ConfigName.ROLLING_WINDOW_TYPE_CONFIG, Collections.emptySet())
            .orElseThrow(
                () ->
                    new ConfigException(
                        "Configuration '"
                            + ConfigName.ROLLING_WINDOW_TYPE_CONFIG
                            + "' must be set."));

    final int rollingWindowSize =
        RollingWindowUtils.extractRollingWindowSize(
                config, rollingWindow, ConfigName.ROLLING_WINDOW_SIZE_CONFIG)
            .orElseThrow(
                () ->
                    new ConfigException(
                        "Configuration '"
                            + ConfigName.ROLLING_WINDOW_SIZE_CONFIG
                            + "' must be set."));

    this.rollingWindowDetails = new RollingWindowDetails(rollingWindow, rollingWindowSize);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }
}
