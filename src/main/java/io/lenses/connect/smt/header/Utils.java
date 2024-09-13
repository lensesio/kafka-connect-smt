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

import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_MICROS;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_MILLIS;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_NANOS;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_SECONDS;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

class Utils {

    static Instant convertToTimestamp(
          Object value, String unixPrecision, Optional<MultiDateTimeFormatter> fromPattern, ZoneId zoneId, Optional<PropsFormatter> propsFormatter) {
    if (value == null) {
      return Instant.now();
    }
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof Long) {
      switch (unixPrecision) {
        case UNIX_PRECISION_SECONDS:
          return new Date(TimeUnit.SECONDS.toMillis((Long) value)).toInstant();
        case UNIX_PRECISION_MICROS:
          return new Date(TimeUnit.MICROSECONDS.toMillis((Long) value)).toInstant();
        case UNIX_PRECISION_NANOS:
          return new Date(TimeUnit.NANOSECONDS.toMillis((Long) value)).toInstant();
        case UNIX_PRECISION_MILLIS:
        default:
          return new Date((Long) value).toInstant();
      }
    }
    if (value instanceof String) {
      return fromPattern
          .map(
              pattern -> {
                try {
                  return pattern.format((String) value, zoneId);
                } catch (Exception e) {
                  throw new DataException(
                      "Could not parse the string timestamp: "
                          + value
                          + " with pattern: "
                          + pattern,
                      e);
                }
              })
          .orElseGet(
              () -> {
                try {
                  return Instant.ofEpochMilli(Long.parseLong((String) value));
                } catch (NumberFormatException e) {
                  throw new DataException("Expected a long, but found " + value + ". Props: " + propsFormatter.map(PropsFormatter::apply).orElse("(No props formatter)"));
                }
              });
    }
    if (value instanceof Date) {
      return ((Date) value).toInstant();
    }
    throw new DataException(
        "Expected an epoch, date or string date, but found " + value.getClass());
  }

  /**
   * Extracts the value for the given field path from a Map
   *
   * @param from the map to extract the value from
   * @param fields the field path
   * @return the value for the given field path
   */
  static Object extractValue(Map<String, Object> from, String[] fields) {
    Map<String, Object> updatedValue =
        requireMap(from, "Extracting value from: " + Arrays.toString(fields));
    for (int i = 0; i < fields.length - 1; i++) {
      updatedValue =
          requireMap(
              updatedValue.get(fields[i]), "Extracting value from: " + Arrays.toString(fields));
      if (updatedValue == null) {
        return null;
      }
    }
    // updatedValue is now the map containing the field to be updated
    // config.fields[config.fields.length-1] is the name of the field to be updated
    return updatedValue.get(fields[fields.length - 1]);
  }

  /**
   * Extracts the value of a given field from the given Struct
   *
   * @param from Struct to extract the value from
   * @param fields the path to the field to extract
   * @return the value of the field, or null if the field does not exist
   */
  static Object extractValue(Struct from, String[] fields) {
    if (from == null) {
      return null;
    }
    Struct updatedValue = from;

    for (int i = 0; i < fields.length - 1; i++) {
      updatedValue = updatedValue.getStruct(fields[i]);
      if (updatedValue == null) {
        return null;
      }
    }
    // updatedValue is now the struct containing the field to be updated
    // config.fields[config.fields.length-1] is the name of the field to be updated
    Field field = updatedValue.schema().field(fields[fields.length - 1]);
    if (field == null) {
      return null;
    }
    return updatedValue.get(fields[fields.length - 1]);
  }

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
