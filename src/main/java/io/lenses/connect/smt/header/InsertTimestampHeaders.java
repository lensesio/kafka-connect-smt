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
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformer which takes the record timestamp and inserts a header year, month, day, hour,
 * minute, second and day. The benefit over the InsertField SMT is that the payload is not modified
 * leading to less memory used and less CPU time.
 *
 * @param <R> the record type
 */
abstract class InsertTimestampHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

  private DateTimeFormatter yearFormat;
  private DateTimeFormatter monthFormat;
  private DateTimeFormatter dayFormat;
  private DateTimeFormatter hourFormat;
  private DateTimeFormatter minuteFormat;
  private DateTimeFormatter secondFormat;
  private DateTimeFormatter dateFormat;
  private String yearHeader;
  private String monthHeader;
  private String dayHeader;
  private String hourHeader;
  private String minuteHeader;
  private String secondHeader;
  private String dateHeader;
  protected Locale locale;
  protected ZoneId timeZone = ZoneId.of("UTC");

  public static ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ConfigName.HEADER_PREFIX_NAME,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_PREFIX_NAME,
              ConfigDef.Importance.HIGH,
              "The prefix to use for the headers inserted. For example, if the prefix is 'wallclock_', the headers inserted will be 'wallclock_year', 'wallclock_month', etc.")
          .define(
              ConfigName.YEAR_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_YEAR_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the year. The default is '"
                  + ConfigName.DEFAULT_YEAR_FORMAT
                  + "'.")
          .define(
              ConfigName.MONTH_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_MONTH_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the month. The default is '"
                  + ConfigName.DEFAULT_MONTH_FORMAT
                  + "'.")
          .define(
              ConfigName.DAY_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_DAY_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the day. The default is '"
                  + ConfigName.DEFAULT_DAY_FORMAT
                  + "'.")
          .define(
              ConfigName.HOUR_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_HOUR_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the hour. The default is '"
                  + ConfigName.DEFAULT_HOUR_FORMAT
                  + "'.")
          .define(
              ConfigName.MINUTE_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_MINUTE_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the minute. The default is '"
                  + ConfigName.DEFAULT_MINUTE_FORMAT
                  + "'.")
          .define(
              ConfigName.SECOND_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_SECOND_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the second. The default is '"
                  + ConfigName.DEFAULT_SECOND_FORMAT
                  + "'.")
          .define(
              ConfigName.DATE_FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_DATE_FORMAT,
              ConfigDef.Importance.HIGH,
              "The format to use for the date. The default is '"
                  + ConfigName.DEFAULT_DATE_FORMAT
                  + "'.")
          .define(
              ConfigName.TIMEZONE,
              ConfigDef.Type.STRING,
              "UTC",
              ConfigDef.Importance.HIGH,
              "The timezone to use.")
          .define(
              ConfigName.LOCALE,
              ConfigDef.Type.STRING,
              ConfigName.DEFAULT_LOCALE,
              ConfigDef.Importance.HIGH,
              "The locale to use.");

  interface ConfigName {
    String HEADER_PREFIX_NAME = "header.prefix.name";

    String DATE_FORMAT_CONFIG = "date.format";
    String YEAR_FORMAT_CONFIG = "year.format";
    String MONTH_FORMAT_CONFIG = "month.format";
    String DAY_FORMAT_CONFIG = "day.format";
    String HOUR_FORMAT_CONFIG = "hour.format";
    String MINUTE_FORMAT_CONFIG = "minute.format";
    String SECOND_FORMAT_CONFIG = "second.format";

    String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    String DEFAULT_YEAR_FORMAT = "yyyy";
    String DEFAULT_MONTH_FORMAT = "MM";
    String DEFAULT_DAY_FORMAT = "dd";
    String DEFAULT_HOUR_FORMAT = "HH";
    String DEFAULT_MINUTE_FORMAT = "mm";
    String DEFAULT_SECOND_FORMAT = "ss";
    String DEFAULT_PREFIX_NAME = "";

    String TIMEZONE = "timezone";

    String LOCALE = "locale";
    String DEFAULT_LOCALE = "en";
  }

  protected InsertTimestampHeaders() {}

  protected abstract Instant getInstant(R r);

  @Override
  public R apply(R r) {
    if (r == null) {
      return null;
    }

    // instant from epoch
    final Instant now = getInstant(r);
    if (now == null) {
      throw new DataException(
          "The timestamp value could not be extracted or is null. Please check the configuration and the record structure.");
    }
    final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(now, timeZone);
    r.headers().addString(yearHeader, yearFormat.format(zonedDateTime));
    r.headers().addString(monthHeader, monthFormat.format(zonedDateTime));
    r.headers().addString(dayHeader, dayFormat.format(zonedDateTime));
    r.headers().addString(hourHeader, hourFormat.format(zonedDateTime));
    r.headers().addString(minuteHeader, minuteFormat.format(zonedDateTime));
    r.headers().addString(secondHeader, secondFormat.format(zonedDateTime));
    r.headers().addString(dateHeader, dateFormat.format(zonedDateTime));
    return r;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  protected void configureInternal(SimpleConfig config) {}

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(config(), props);
    final String timeZoneStr = config.getString(ConfigName.TIMEZONE);
    timeZone = TimeZone.getTimeZone(timeZoneStr).toZoneId();
    String prefixName = config.getString(ConfigName.HEADER_PREFIX_NAME);
    yearHeader = prefixName + "year";
    monthHeader = prefixName + "month";
    dayHeader = prefixName + "day";
    hourHeader = prefixName + "hour";
    minuteHeader = prefixName + "minute";
    secondHeader = prefixName + "second";
    dateHeader = prefixName + "date";
    locale = new Locale(config.getString(ConfigName.LOCALE));
    yearFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.YEAR_FORMAT_CONFIG),
                ConfigName.YEAR_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    monthFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.MONTH_FORMAT_CONFIG),
                ConfigName.MONTH_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    dayFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.DAY_FORMAT_CONFIG),
                ConfigName.DAY_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    hourFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.HOUR_FORMAT_CONFIG),
                ConfigName.HOUR_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    minuteFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.MINUTE_FORMAT_CONFIG),
                ConfigName.MINUTE_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    secondFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.SECOND_FORMAT_CONFIG),
                ConfigName.SECOND_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    dateFormat =
        createDateTimeFormatter(
                config.getString(ConfigName.DATE_FORMAT_CONFIG),
                ConfigName.DATE_FORMAT_CONFIG,
                locale)
            .withZone(timeZone);
    configureInternal(config);
  }

  public static DateTimeFormatter createDateTimeFormatter(
      String patternConfig, String configName, Locale locale) {
    try {
      return DateTimeFormatter.ofPattern(patternConfig, locale);
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Configuration '" + configName + "' is not a valid date format.");
    }
  }
}
