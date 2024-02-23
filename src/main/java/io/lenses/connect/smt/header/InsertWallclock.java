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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformation that inserts a header with the current wallclock time. This implementation is
 * less costly than the InsertField SMT where the payload is modified leading to more memory used a
 * more CPU time. The header value can be either epoch or a formatted string; default is formatted
 * string.
 *
 * <p>Example:
 *
 * <pre>{@code
 * "transforms": "insertWallclockHeader",
 * "transforms.insertWallclockHeader.type": "io.lenses.connect.smt.header.InsertWallclock",
 * "transforms.insertWallclockHeader.header.name": "wallclock",
 * "transforms.insertWallclockHeader.value.type": "epoch"
 *
 * }</pre>
 *
 * <pre>{@code
 * "transforms": "insertWallclockHeader",
 * "transforms.insertWallclockHeader.type": "io.lenses.connect.smt.header.InsertWallclock",
 * "transforms.insertWallclockHeader.header.name": "wallclock",
 * "transforms.insertWallclockHeader.value.type": "format",
 * "transforms.insertWallclockHeader.format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
 * "transforms.insertWallclockHeader.timezone": "Europe/Paris"
 *
 * }</pre>
 *
 * @param <R> - the record type
 */
public class InsertWallclock<R extends ConnectRecord<R>> implements Transformation<R> {
  private String headerKey;
  private DateTimeFormatter format;

  private Supplier<String> valueExtractorF;

  private Supplier<Instant> instantF = Instant::now;

  private TimeZone timeZone = TimeZone.getTimeZone("UTC");
  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  /**
   * Used to testing only to inject the instant value.
   *
   * @param instantF - the instant supplier
   */
  void setInstantF(Supplier<Instant> instantF) {
    this.instantF = instantF;
  }

  private interface ConfigName {
    String HEADER_KEY = "header.name";
    String VALUE_TYPE = "value.type";

    String VALUE_TYPE_EPOCH = "epoch";
    String VALUE_TYPE_FORMAT = "format";
    String FORMAT = "format";

    String TIMEZONE = "timezone";
  }

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ConfigName.HEADER_KEY,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "Header key name . ")
          .define(
              ConfigName.VALUE_TYPE,
              ConfigDef.Type.STRING,
              "format",
              (s, o) -> {
                if (o == null) {
                  throw new ConfigException(
                      "'" + ConfigName.VALUE_TYPE_EPOCH + "' value need to be 'epoch' or 'format'");
                }
                String value = ((String) o).toLowerCase(Locale.ROOT);
                if (!value.equals(ConfigName.VALUE_TYPE_EPOCH)
                    && !value.equals(ConfigName.VALUE_TYPE_FORMAT)) {
                  throw new ConfigException(
                      "'" + ConfigName.VALUE_TYPE + "' must be set to either 'epoch' or 'string'");
                }
              },
              ConfigDef.Importance.MEDIUM,
              "Sets the header value inserted. It can be epoch or string. "
                  + "If string is used, then the '"
                  + ConfigName.FORMAT
                  + "' setting is required.")
          .define(
              ConfigName.FORMAT,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "Sets the format of the header value inserted if the type was set to string. "
                  + "It can be any valid java date format.")
          .define(
              ConfigName.TIMEZONE,
              ConfigDef.Type.STRING,
              "UTC",
              ConfigDef.Importance.MEDIUM,
              "Sets the timezone of the header value inserted if the type was set to string. ");

  @Override
  public R apply(R r) {
    if (r == null) {
      return null;
    }
    final String value = valueExtractorF.get();
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
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    headerKey = config.getString(ConfigName.HEADER_KEY);
    if (headerKey == null) {
      throw new ConfigException("Configuration '" + ConfigName.HEADER_KEY + "' must be set.");
    } else if (headerKey.isEmpty()) {
      throw new ConfigException("Configuration '" + ConfigName.HEADER_KEY + "' must not be empty.");
    }
    String valueType = config.getString(ConfigName.VALUE_TYPE);
    if (valueType == null) {
      throw new ConfigException("Configuration '" + ConfigName.VALUE_TYPE + "' must be set.");
    } else if (valueType.isEmpty()) {
      throw new ConfigException("Configuration '" + ConfigName.VALUE_TYPE + "' must not be empty.");
    } else if (!valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_EPOCH)
        && !valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_FORMAT)) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.VALUE_TYPE
              + "' must be set to either 'epoch' or 'format'.");
    }
    final String timezoneStr = config.getString(ConfigName.TIMEZONE);
    try {
      timeZone = TimeZone.getTimeZone(timezoneStr);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          "Configuration '" + ConfigName.TIMEZONE + "' is not a valid timezone.");
    }
    if (valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_FORMAT)) {
      final String pattern = config.getString(ConfigName.FORMAT);
      if (pattern == null) {
        format = DEFAULT_FORMATTER;
      } else {
        try {
          format = DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException e) {
          throw new ConfigException(
              "Configuration '" + ConfigName.FORMAT + "' is not a valid date format.");
        }
      }
      format = format.withZone(timeZone.toZoneId());
      valueExtractorF = this::getFormattedValue;
    } else {
      if (!timeZone.getID().equals(Constants.UTC.getId())) {
        throw new ConfigException(
            "Configuration '"
                + ConfigName.TIMEZONE
                + "' must be set to 'UTC' when 'value.type' is set to 'epoch'.");
      }
      valueExtractorF = this::getEpochValue;
    }
  }

  private String getEpochValue() {
    Instant wallclock = instantF.get();
    return String.valueOf(wallclock.toEpochMilli());
  }

  private String getFormattedValue() {
    Instant wallclock = instantF.get();

    OffsetDateTime dateTime = OffsetDateTime.ofInstant(wallclock, ZoneOffset.UTC);
    return format.format(dateTime);
  }
}
