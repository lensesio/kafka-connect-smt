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

import static io.lenses.connect.smt.header.FieldTypeConstants.KEY_FIELD;
import static io.lenses.connect.smt.header.FieldTypeConstants.VALUE_FIELD;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_MICROS;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_MILLIS;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_NANOS;
import static io.lenses.connect.smt.header.UnixPrecisionConstants.UNIX_PRECISION_SECONDS;
import static io.lenses.connect.smt.header.Utils.convertToTimestamp;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

class RecordFieldTimestamp<R extends ConnectRecord<R>> {
  private static final String FIELD_CONFIG = "field";
  private static final String FORMAT_FROM_CONFIG = "format.from.pattern";

  public static final String UNIX_PRECISION_CONFIG = "unix.precision";
  private static final String UNIX_PRECISION_DEFAULT = "milliseconds";
  private final FieldTypeUtils.FieldTypeAndFields fieldTypeAndFields;
  private final Optional<MultiDateTimeFormatter> fromPattern;
  private final String unixPrecision;
  private final ZoneId timeZone;

  private final Optional<PropsFormatter> propsFormatter;

  private RecordFieldTimestamp(
      FieldTypeUtils.FieldTypeAndFields fieldTypeAndFields,
      Optional<MultiDateTimeFormatter> fromPattern,
      String unixPrecision,
      ZoneId timeZone,
      Optional<PropsFormatter> propsFormatter) {

    this.fieldTypeAndFields = fieldTypeAndFields;
    this.fromPattern = fromPattern;
    this.unixPrecision = unixPrecision;
    this.timeZone = timeZone;
    this.propsFormatter = propsFormatter;
  }

  public FieldTypeUtils.FieldTypeAndFields getFieldTypeAndFields() {
    return fieldTypeAndFields;
  }

  public String getUnixPrecision() {
    return unixPrecision;
  }

  /**
   * The check for null happens in InsertTimestampHeaders
   *
   * @param r the record
   * @return the instant
   */
  public Instant getInstant(R r) {
    FieldType fieldType = fieldTypeAndFields.getFieldType();
    if (fieldType == FieldType.TIMESTAMP) {
      return r.timestamp() == null ? Instant.now() : Instant.ofEpochMilli(r.timestamp());
    } else {

      final Object value = operatingValue(r);
      if (value == null) {
        return null;
      }
      if (fieldTypeAndFields.getFields().length == 0) {
        return convertToTimestamp(value, unixPrecision, fromPattern, timeZone, propsFormatter);
      }
      final Schema schema = operatingSchema(r);
      Object extractedValue;
      if (schema == null) {
        // there's schemaless data; the input expected is a Map
        extractedValue =
            Utils.extractValue(
                requireMap(
                    value,
                    "Extracting field value for:"
                        + Arrays.toString(fieldTypeAndFields.getFields())),
                fieldTypeAndFields.getFields());
      } else if (value instanceof Struct) {
        extractedValue = Utils.extractValue((Struct) value, fieldTypeAndFields.getFields());
      } else {

        throw new DataException(
            "The SMT is configured to extract the data from: "
                + Arrays.toString(fieldTypeAndFields.getFields())
                + " thus it requires a Struct value. Found: "
                + value.getClass().getName()
                + " instead.");
      }

      return convertToTimestamp(
          extractedValue, unixPrecision, fromPattern, timeZone, propsFormatter);
    }
  }

  private Schema operatingSchema(R record) {
    switch (fieldTypeAndFields.getFieldType()) {
      case KEY:
        return record.keySchema();
      case TIMESTAMP:
        return Timestamp.SCHEMA;
      default:
        return record.valueSchema();
    }
  }

  private Object operatingValue(R record) {
    if (fieldTypeAndFields.getFieldType() == FieldType.TIMESTAMP) {
      return record.timestamp();
    }
    if (fieldTypeAndFields.getFieldType() == FieldType.KEY) {
      return record.key();
    }
    return record.value();
  }

  public static <R extends ConnectRecord<R>> RecordFieldTimestamp<R> create(
      SimpleConfig config, ZoneId zoneId, Locale locale) {
    String fieldConfig = config.getString(FIELD_CONFIG);
    if (fieldConfig == null || fieldConfig.isEmpty()) {
      fieldConfig = FieldTypeConstants.VALUE_FIELD;
    }

    final FieldTypeUtils.FieldTypeAndFields fieldTypeAndFields =
        FieldTypeUtils.extractFieldTypeAndFields(fieldConfig);

    final String unixPrecision =
        Optional.ofNullable(config.getString(UNIX_PRECISION_CONFIG)).orElse(UNIX_PRECISION_DEFAULT);

    final Optional<MultiDateTimeFormatter> fromPattern =
        Optional.ofNullable(config.getList(FORMAT_FROM_CONFIG))
            .map(
                patterns ->
                    MultiDateTimeFormatter.createDateTimeFormatter(
                        patterns, FORMAT_FROM_CONFIG, locale));

    return new RecordFieldTimestamp<>(
        fieldTypeAndFields,
        fromPattern,
        unixPrecision,
        zoneId,
        Optional.of(new PropsFormatter(config)));
  }

  public static ConfigDef extendConfigDef(ConfigDef from) {
    return from.define(
            FIELD_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            "The field path containing the timestamp, or empty if the entire value"
                + " is a timestamp. Prefix the path with the literal string '"
                + KEY_FIELD
                + "' or '"
                + VALUE_FIELD
                + "' to specify the record key or value."
                + "If no prefix is specified, the default is '"
                + VALUE_FIELD
                + "'.")
        .define(
            FORMAT_FROM_CONFIG,
            ConfigDef.Type.LIST,
            null,
            ConfigDef.Importance.MEDIUM,
            "A DateTimeFormatter-compatible format for the timestamp. Used to parse the"
                + " input if the input is a string.")
        .define(
            UNIX_PRECISION_CONFIG,
            ConfigDef.Type.STRING,
            UNIX_PRECISION_DEFAULT,
            ConfigDef.ValidString.in(
                UNIX_PRECISION_NANOS, UNIX_PRECISION_MICROS,
                UNIX_PRECISION_MILLIS, UNIX_PRECISION_SECONDS),
            ConfigDef.Importance.LOW,
            "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, "
                + "or nanoseconds. Used to parse the input if the input is a Long. This SMT will cause precision loss during "
                + "conversions from, and to, values with sub-millisecond components.");
  }
}
