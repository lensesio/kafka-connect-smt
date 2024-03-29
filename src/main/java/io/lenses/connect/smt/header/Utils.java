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

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.common.config.ConfigException;

class Utils {

  static DateTimeFormatter getDateFormat(String formatPattern, ZoneId zoneId) {
    if (formatPattern == null) {
      return null;
    }
    DateTimeFormatter format = null;
    if (!isBlank(formatPattern)) {
      try {
        format = DateTimeFormatter.ofPattern(formatPattern).withZone(zoneId);
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
            "TimestampConverter requires a DateTimeFormatter-compatible pattern "
                + "for string timestamps: "
                + formatPattern,
            e);
      }
    }
    return format;
  }

  public static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }
}
