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

  public static ConfigDef CONFIG_DEF =
      RecordFieldTimestamp.extendConfigDef(InsertRollingTimestampHeaders.CONFIG_DEF);

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
