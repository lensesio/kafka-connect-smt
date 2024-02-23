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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformation that inserts a header with the current wallclock time bound by a time window.
 * For example, the requirement is to capture rolling data every 15 minutes, or 5 hours. This
 * implementation is less costly than the InsertField SMT where the payload is modified leading to
 * more memory used a more CPU time.
 */
public class InsertRollingWallclock<R extends ConnectRecord<R>> implements Transformation<R> {

  private String headerKey;
  private DateTimeFormatter format;

  private RollingWindowDetails rollingWindowDetails;

  private Supplier<Instant> instantF = Instant::now;

  private Supplier<String> valueExtractorF;

  // Rolling window set to 15minutes by default
  private static final int DEFAULT_ROLLING_WINDOW_VALUE = 15;
  private static final RollingWindow DEFAULT_ROLLING_WINDOW = RollingWindow.MINUTES;
  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
  private ZoneId timezone = Constants.UTC;

  private interface ConfigName {
    String HEADER_NAME_CONFIG = "header.name";
    String ROLLING_WINDOW_TYPE_CONFIG = "rolling.window.type";
    String FORMAT_CONFIG = "format";

    String VALUE_TYPE_CONFIG = "value.type";

    String ROLLING_WINDOW_SIZE_CONFIG = "rolling.window.size";

    String VALUE_TYPE_EPOCH = "epoch";
    String VALUE_TYPE_FORMAT = "format";

    String TIMEZONE_CONFIG = "timezone";
  }

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ConfigName.HEADER_NAME_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "The header name")
          .define(
              ConfigName.ROLLING_WINDOW_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              DEFAULT_ROLLING_WINDOW.name(),
              ConfigDef.Importance.HIGH,
              "The rolling window type. The allowed values are hours, minutes or seconds.")
          .define(
              ConfigName.FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "The format of the date.")
          .define(
              ConfigName.VALUE_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              "format",
              ConfigDef.Importance.HIGH,
              "The type of the value, either 'epoch' or 'format'.")
          .define(
              ConfigName.ROLLING_WINDOW_SIZE_CONFIG,
              ConfigDef.Type.INT,
              DEFAULT_ROLLING_WINDOW_VALUE,
              ConfigDef.Importance.HIGH,
              "The rolling window size. For example, if the rolling window is set to 'minutes' "
                  + "and the rolling window value is set to 15, then the rolling window "
                  + "is 15 minutes.")
          .define(
              ConfigName.TIMEZONE_CONFIG,
              ConfigDef.Type.STRING,
              "UTC",
              ConfigDef.Importance.HIGH,
              "The timezone used when 'value.type' is set to format.");

  /**
   * Used to testing only to inject the instant value.
   *
   * @param instantF - the instant supplier
   */
  void setInstantF(Supplier<Instant> instantF) {
    this.instantF = instantF;
  }

  @Override
  public R apply(R r) {
    if (r == null) {
      return null;
    }
    String value = valueExtractorF.get();
    r.headers().addString(headerKey, value);
    return r;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    headerKey = config.getString(ConfigName.HEADER_NAME_CONFIG);
    if (headerKey == null) {
      throw new ConfigException(
          "Configuration '" + ConfigName.HEADER_NAME_CONFIG + "' must be set.");
    } else if (headerKey.isEmpty()) {
      throw new ConfigException(
          "Configuration '" + ConfigName.HEADER_NAME_CONFIG + "' must not be empty.");
    }
    String valueType = config.getString(ConfigName.VALUE_TYPE_CONFIG);
    if (valueType == null) {
      throw new ConfigException(
          "Configuration '" + ConfigName.VALUE_TYPE_CONFIG + "' must be set.");
    } else if (valueType.isEmpty()) {
      throw new ConfigException(
          "Configuration '" + ConfigName.VALUE_TYPE_CONFIG + "' must not be empty.");
    } else if (!valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_EPOCH)
        && !valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_FORMAT)) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.VALUE_TYPE_CONFIG
              + "' must be set to either 'epoch' or 'format'.");
    }
    final String timezoneStr = config.getString(ConfigName.TIMEZONE_CONFIG);
    try {
      this.timezone = TimeZone.getTimeZone(timezoneStr).toZoneId();
    } catch (Exception e) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.TIMEZONE_CONFIG
              + "' is not a valid timezone. It can be any valid java timezone.");
    }
    if (!this.timezone.getId().equals(Constants.UTC.getId())
        && valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_EPOCH)) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.TIMEZONE_CONFIG
              + "' is not allowed to be set to a value other than UTC when '"
              + ConfigName.VALUE_TYPE_CONFIG
              + "' is set to 'epoch'.");
    }
    if (valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_FORMAT)) {
      final String pattern = config.getString(ConfigName.FORMAT_CONFIG);
      if (pattern == null) {
        format = DEFAULT_FORMATTER;
      } else {
        try {
          format = DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException e) {
          throw new ConfigException(
              "Configuration '" + ConfigName.FORMAT_CONFIG + "' is not a valid date format.", e);
        }
      }
      valueExtractorF = this::getFormattedValue;
      format = format.withZone(timezone);
    } else {
      valueExtractorF = this::getEpochValue;
    }

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

  private String getEpochValue() {
    Instant wallclock = rollingWindowDetails.adjust(instantF.get());
    return String.valueOf(wallclock.toEpochMilli());
  }

  private String getFormattedValue() {
    Instant now = instantF.get();
    Instant wallclock = rollingWindowDetails.adjust(now);

    OffsetDateTime dateTime = OffsetDateTime.ofInstant(wallclock, ZoneOffset.UTC);
    return format.format(dateTime);
  }
}
