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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;

class MultiDateTimeFormatter {

  private final List<DateTimeFormatter> formatters;
  private final List<String> patterns;
  private final Boolean returnNowIfNull;

  public MultiDateTimeFormatter(
      List<String> patterns, List<DateTimeFormatter> formatters, Boolean returnNowIfNull) {
    this.patterns = patterns;
    this.formatters = formatters;
    this.returnNowIfNull = returnNowIfNull;
  }

  public Instant format(String value, ZoneId zoneId) {
    if (value == null && returnNowIfNull) {
      return LocalDateTime.now().atZone(zoneId).toInstant();
    } else if (value == null) {
      throw new DateTimeParseException("No valid date time provided", "null", 0);
    }
    for (DateTimeFormatter formatter : formatters) {
      try {
        LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
        return localDateTime.atZone(zoneId).toInstant();
      } catch (DateTimeParseException dtpe) {
        // ignore exception and use fallback
      }
    }
    throw new DateTimeParseException("Cannot parse date with any formats", value, 0);
  }

  public String getDisplayPatterns() {
    return String.join(", ", patterns);
  }

  private static DateTimeFormatter createFormatter(
      String pattern, String configName, Locale locale, ZoneId zoneId) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
      if (locale != null) {
        formatter = formatter.withLocale(locale);
      }
      if (zoneId != null) {
        formatter = formatter.withZone(zoneId);
      }
      return formatter;
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Configuration '" + configName + "' is not a valid date format.");
    }
  }

  public static MultiDateTimeFormatter createDateTimeFormatter(
      List<String> patternConfigs, String configName, Locale locale) {

    return new MultiDateTimeFormatter(
        patternConfigs,
        patternConfigs.stream()
            .map(patternConfig -> createFormatter(patternConfig, configName, locale, null))
            .collect(Collectors.toUnmodifiableList()),
        false);
  }

  public static MultiDateTimeFormatter createDateTimeFormatter(
      List<String> patternConfigs, String configName, ZoneId zoneId) {

    return new MultiDateTimeFormatter(
        patternConfigs,
        patternConfigs.stream()
            .map(patternConfig -> createFormatter(patternConfig, configName, null, zoneId))
            .collect(Collectors.toUnmodifiableList()),
        true);
  }
}
