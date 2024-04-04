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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformer which takes a record filed of type timestamp and inserts a header year, month, day,
 * hour, minute, second and date.
 *
 * @param <R> the record type
 */
public class InsertRollingFieldTimestampHeaders<R extends ConnectRecord<R>>
    extends InsertRollingTimestampHeaders<R> {
  private RecordFieldTimestamp<R> fieldTimestamp;

  public static ConfigDef CONFIG_DEF;

  static {
    // The code would be
    // RecordFieldTimestamp.extendConfigDef(InsertRollingTimestampHeaders.CONFIG_DEF);
    // However Connect runtime gets badly confused for reasons not understood.
    // Connect runtime is thinking that the field setting is defined already which is not the case
    // The workaround is to redefine all the ConfigDef settings here to avoid the Connect runtime
    // nonsense
    ConfigDef replicated =
        new ConfigDef()
            .define(
                InsertTimestampHeaders.ConfigName.HEADER_PREFIX_NAME,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_PREFIX_NAME,
                ConfigDef.Importance.HIGH,
                "The prefix to use for the headers inserted. For example, if the prefix is 'wallclock_', the headers inserted will be 'wallclock_year', 'wallclock_month', etc.")
            .define(
                InsertTimestampHeaders.ConfigName.YEAR_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_YEAR_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the year. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_YEAR_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.MONTH_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_MONTH_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the month. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_MONTH_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.DAY_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_DAY_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the day. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_DAY_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.HOUR_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_HOUR_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the hour. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_HOUR_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.MINUTE_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_MINUTE_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the minute. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_MINUTE_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.SECOND_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_SECOND_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the second. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_SECOND_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.DATE_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_DATE_FORMAT,
                ConfigDef.Importance.HIGH,
                "The format to use for the date. The default is '"
                    + InsertTimestampHeaders.ConfigName.DEFAULT_DATE_FORMAT
                    + "'.")
            .define(
                InsertTimestampHeaders.ConfigName.TIMEZONE,
                ConfigDef.Type.STRING,
                "UTC",
                ConfigDef.Importance.HIGH,
                "The timezone to use.")
            .define(
                InsertTimestampHeaders.ConfigName.LOCALE,
                ConfigDef.Type.STRING,
                InsertTimestampHeaders.ConfigName.DEFAULT_LOCALE,
                ConfigDef.Importance.HIGH,
                "The locale to use.")
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

    CONFIG_DEF = RecordFieldTimestamp.extendConfigDef(replicated);
  }

  public InsertRollingFieldTimestampHeaders() {
    super();
  }

  @Override
  protected Instant getInstantInternal(R r) {
    return fieldTimestamp.getInstant(r);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  protected void configureInternal(SimpleConfig config) {
    super.configureInternal(config);
    fieldTimestamp = RecordFieldTimestamp.create(config, timeZone, locale);
  }
}
