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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformer allowing to insert a header with the current wallclock year, month, day, hour,
 * minute, or second. The benefit over the InsertField SMT is that the payload is not modified
 * leading to less memory used and less CPU time.
 *
 * @param <R> the record type
 */
public class InsertWallclockDateTimePart<R extends ConnectRecord<R>> implements Transformation<R> {

  private String headerName;

  // Used for testing only to inject the instant value
  private Supplier<Instant> instantF = Instant::now;

  private ZoneId timeZone = ZoneId.of("UTC");

  private Function<ZonedDateTime, String> valueExtractorF;

  void setInstantF(Supplier<Instant> instantF) {
    this.instantF = instantF;
  }

  public static ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ConfigName.HEADER_NAME,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "The name of the header to insert.")
          .define(
              ConfigName.DATE_TIME_PART,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "The date time part to insert. Valid values are:"
                  + Arrays.stream(DateTimePart.values())
                      .map(Enum::name)
                      .reduce((a, b) -> a + ", " + b)
                      .orElse(""))
          .define(
              ConfigName.TIMEZONE,
              ConfigDef.Type.STRING,
              "UTC",
              ConfigDef.Importance.HIGH,
              "The timezone to use.");

  interface ConfigName {
    String HEADER_NAME = "header.name";
    String DATE_TIME_PART = "date.time.part";

    String TIMEZONE = "timezone";
  }

  enum DateTimePart {
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND
  }

  @Override
  public R apply(R r) {
    if (r == null) {
      return null;
    }

    Instant now = instantF.get();
    final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(now, timeZone);
    final String value = valueExtractorF.apply(zonedDateTime);
    r.headers().addString(headerName, value);
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
    final String timeZoneStr = config.getString(ConfigName.TIMEZONE);
    try {
      timeZone = TimeZone.getTimeZone(timeZoneStr).toZoneId();
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          "Configuration '" + ConfigName.TIMEZONE + "' is not a valid timezone.");
    }
    headerName = config.getString(ConfigName.HEADER_NAME);
    DateTimePart dateTimePart;
    try {
      dateTimePart =
          DateTimePart.valueOf(
              config.getString(ConfigName.DATE_TIME_PART).toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          "Invalid '"
              + ConfigName.DATE_TIME_PART
              + "': "
              + config.getString(ConfigName.DATE_TIME_PART)
              + ". Valid values are: "
              + Arrays.stream(DateTimePart.values())
                  .map(Enum::name)
                  .reduce((a, b) -> a + ", " + b)
                  .orElse(""));
    }
    // initialize the value extractor
    switch (dateTimePart) {
      case YEAR:
        valueExtractorF = InsertWallclockDateTimePart::getYear;
        break;
      case MONTH:
        valueExtractorF = InsertWallclockDateTimePart::getMonth;
        break;
      case DAY:
        valueExtractorF = InsertWallclockDateTimePart::getDay;
        break;
      case HOUR:
        valueExtractorF = InsertWallclockDateTimePart::getHour;
        break;
      case MINUTE:
        valueExtractorF = InsertWallclockDateTimePart::getMinute;
        break;
      case SECOND:
        valueExtractorF = InsertWallclockDateTimePart::getSecond;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + dateTimePart);
    }
  }

  private static String getYear(ZonedDateTime time) {
    return String.valueOf(time.getYear());
  }

  private static String getMonth(ZonedDateTime time) {
    return String.valueOf(time.getMonthValue());
  }

  private static String getDay(ZonedDateTime time) {
    return String.valueOf(time.getDayOfMonth());
  }

  private static String getHour(ZonedDateTime time) {
    return String.valueOf(time.getHour());
  }

  private static String getMinute(ZonedDateTime time) {
    return String.valueOf(time.getMinute());
  }

  private static String getSecond(ZonedDateTime time) {
    return String.valueOf(time.getSecond());
  }
}
