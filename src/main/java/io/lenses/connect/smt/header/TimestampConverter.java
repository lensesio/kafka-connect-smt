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
import static io.lenses.connect.smt.header.Utils.isBlank;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformer which converts a timestamp value field and inserts it as a header. It supports
 * scenarios like time based rolling windows for partitioning when sinking to a storage like S3,
 * while avoiding the memory and CPU overhead of the InsertField SMT.
 *
 * @param <R> - the record type
 */
public final class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {
  public static final String FIELD_CONFIG = "field";
  public static final String HEADER_NAME_CONFIG = "header.name";

  public static final String TARGET_TYPE_CONFIG = "target.type";

  public static final String ROLLING_WINDOW_TYPE_CONFIG = "rolling.window.type";
  public static final String ROLLING_WINDOW_SIZE_CONFIG = "rolling.window.size";
  public static final String FORMAT_FROM_CONFIG = "format.from.pattern";
  public static final String FORMAT_TO_CONFIG = "format.to.pattern";

  public static final String TARGET_TIMEZONE_CONFIG = "target.timezone";

  public static final String UNIX_PRECISION_CONFIG = "unix.precision";
  private static final String UNIX_PRECISION_DEFAULT = "milliseconds";

  private static final String PURPOSE = "converting timestamp formats";

  private static final String TYPE_STRING = "string";
  private static final String TYPE_UNIX = "unix";
  private static final String TYPE_DATE = "Date";
  private static final String TYPE_TIME = "Time";
  private static final String TYPE_TIMESTAMP = "Timestamp";

  public static final Schema OPTIONAL_DATE_SCHEMA =
      org.apache.kafka.connect.data.Date.builder().optional().schema();
  public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
  public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              FIELD_CONFIG,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The field path containing the timestamp, or empty if the entire value"
                  + " is a timestamp. Prefix the path with the literal string '"
                  + FieldTypeConstants.KEY_FIELD
                  + "' or '"
                  + FieldTypeConstants.VALUE_FIELD
                  + "' to specify the record key or value."
                  + "If no prefix is specified, the default is '"
                  + FieldTypeConstants.VALUE_FIELD
                  + "'.")
          .define(
              HEADER_NAME_CONFIG,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "The name of the header to insert the timestamp into." + " It cannot be null.")
          .define(
              TARGET_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.ValidString.in(
                  TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP),
              ConfigDef.Importance.HIGH,
              "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
          .define(
              FORMAT_FROM_CONFIG,
              ConfigDef.Type.LIST,
              null,
              ConfigDef.Importance.MEDIUM,
              "A DateTimeFormatter-compatible format for the timestamp. Used to parse the"
                  + " input if the input is a string.")
          .define(
              FORMAT_TO_CONFIG,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "A DateTimeFormatter-compatible format for the timestamp. Used to generate the "
                  + " output when '"
                  + TARGET_TYPE_CONFIG
                  + "' is set to string.")
          .define(
              UNIX_PRECISION_CONFIG,
              ConfigDef.Type.STRING,
              UNIX_PRECISION_DEFAULT,
              ConfigDef.ValidString.in(
                  UNIX_PRECISION_NANOS, UNIX_PRECISION_MICROS,
                  UNIX_PRECISION_MILLIS, UNIX_PRECISION_SECONDS),
              ConfigDef.Importance.LOW,
              "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, "
                  + "or nanoseconds. Used to generate the output when type=unix or used to parse "
                  + "the input if the input is a Long. This SMT will cause precision loss during "
                  + "conversions from, and to, values with sub-millisecond components.")
          .define(
              ROLLING_WINDOW_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              "none",
              ConfigDef.ValidString.in("none", "hours", "minutes", "seconds"),
              ConfigDef.Importance.LOW,
              "An optional desired rolling window type: 'none', 'hours', 'minutes' or 'seconds'."
                  + " Default is 'none'.")
          .define(
              ROLLING_WINDOW_SIZE_CONFIG,
              ConfigDef.Type.INT,
              null,
              ConfigDef.Importance.LOW,
              "The rolling window size. For example, if the rolling window is set to 'minutes' "
                  + "size is set to 15.")
          .define(
              TARGET_TIMEZONE_CONFIG,
              ConfigDef.Type.STRING,
              "UTC",
              ConfigDef.Importance.LOW,
              "The timezone to use for the timestamp conversion.");

  private interface TimestampTranslator {
    /** Convert from the type-specific format to the universal java.util.Date format */
    Date toRaw(Config config, Object orig);

    /** Get the schema for this format. */
    Schema typeSchema(boolean isOptional);

    /** Convert from the universal java.util.Date format to the type-specific format */
    Object toType(Config config, Date orig);
  }

  private static final Map<String, TimestampTranslator> TRANSLATORS = new HashMap<>();

  static {
    TRANSLATORS.put(
        TYPE_STRING,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof String)) {
              throw new DataException(
                  "Expected string timestamp to be a String, but found " + orig.getClass());
            }
            if (config.fromFormat == null) {
              throw new DataException(
                  "Input format is not specified, and input data is string. "
                      + "Please specify the input format using the '"
                      + FORMAT_FROM_CONFIG
                      + "' configuration property.");
            }
            try {
              return Date.from(config.fromFormat.format((String) orig, ZoneOffset.UTC));
            } catch (DateTimeParseException e) {
              throw new DataException(
                  "Could not parse timestamp: value ("
                      + orig
                      + ") does not match any patterns ("
                      + config.fromFormat.getDisplayPatterns()
                      + ")",
                  e);
            }
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
          }

          @Override
          public String toType(Config config, Date orig) {
            return config.toFormat.format(orig.toInstant());
          }
        });

    TRANSLATORS.put(
        TYPE_UNIX,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (orig instanceof String) {
              // try to parse it as a long
              try {
                orig = Long.parseLong((String) orig);
              } catch (NumberFormatException e) {
                throw new DataException(
                    "Expected Unix timestamp to be a string representation of an epoch LONG, but found "
                        + orig);
              }
            }
            if (!(orig instanceof Long)) {
              throw new DataException(
                  "Expected Unix timestamp to be a Long, but found " + orig.getClass());
            }
            long unixTime = (Long) orig;
            switch (config.unixPrecision) {
              case UNIX_PRECISION_SECONDS:
                return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.SECONDS.toMillis(unixTime));
              case UNIX_PRECISION_MICROS:
                return Timestamp.toLogical(
                    Timestamp.SCHEMA, TimeUnit.MICROSECONDS.toMillis(unixTime));
              case UNIX_PRECISION_NANOS:
                return Timestamp.toLogical(
                    Timestamp.SCHEMA, TimeUnit.NANOSECONDS.toMillis(unixTime));
              case UNIX_PRECISION_MILLIS:
              default:
                return Timestamp.toLogical(Timestamp.SCHEMA, unixTime);
            }
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
          }

          @Override
          public Long toType(Config config, Date orig) {
            long unixTimeMillis = Timestamp.fromLogical(Timestamp.SCHEMA, orig);
            switch (config.unixPrecision) {
              case UNIX_PRECISION_SECONDS:
                return TimeUnit.MILLISECONDS.toSeconds(unixTimeMillis);
              case UNIX_PRECISION_MICROS:
                return TimeUnit.MILLISECONDS.toMicros(unixTimeMillis);
              case UNIX_PRECISION_NANOS:
                return TimeUnit.MILLISECONDS.toNanos(unixTimeMillis);
              case UNIX_PRECISION_MILLIS:
              default:
                return unixTimeMillis;
            }
          }
        });

    TRANSLATORS.put(
        TYPE_DATE,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof Date)) {
              throw new DataException(
                  "Expected Date to be a java.util.Date, but found " + orig.getClass());
            }
            // Already represented as a java.util.Date and Connect Dates are a subset of valid
            // java.util.Date values
            return (Date) orig;
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
          }

          @Override
          public Date toType(Config config, Date orig) {
            ZonedDateTime truncated =
                ZonedDateTime.of(
                    orig.toInstant().atZone(config.targetTimeZoneId).toLocalDate(),
                    LocalTime.MIDNIGHT,
                    Constants.UTC_ZONE_ID);

            return Date.from(truncated.toInstant());
          }
        });

    TRANSLATORS.put(
        TYPE_TIME,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof Date)) {
              throw new DataException(
                  "Expected Time to be a java.util.Date, but found " + orig.getClass());
            }
            // Already represented as a java.util.Date and Connect Times are a subset of valid
            // java.util.Date values
            return (Date) orig;
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
          }

          @Override
          public Date toType(Config config, Date orig) {
            ZonedDateTime zonedDateTime = orig.toInstant().atZone(config.targetTimeZoneId);
            return Date.from(
                ZonedDateTime.of(
                        LocalDate.of(1970, 1, 1),
                        zonedDateTime.toLocalTime(),
                        Constants.UTC_ZONE_ID)
                    .toInstant());
          }
        });

    TRANSLATORS.put(
        TYPE_TIMESTAMP,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            // long epoch to date
            if (orig instanceof Long) {
              // if precision is not specified, assume milliseconds
              // otherwise convert to milliseconds
              switch (config.unixPrecision) {
                case UNIX_PRECISION_SECONDS:
                  return new Date(TimeUnit.SECONDS.toMillis((Long) orig));
                case UNIX_PRECISION_MICROS:
                  return new Date(TimeUnit.MICROSECONDS.toMillis((Long) orig));
                case UNIX_PRECISION_NANOS:
                  return new Date(TimeUnit.NANOSECONDS.toMillis((Long) orig));
                case UNIX_PRECISION_MILLIS:
                default:
                  return new Date((Long) orig);
              }
            }
            if (!(orig instanceof Date)) {
              throw new DataException(
                  "Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
            }
            return (Date) orig;
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
          }

          @Override
          public Date toType(Config config, Date orig) {
            return orig;
          }
        });
  }

  // This is a bit unusual, but allows the transformation config to be passed to static anonymous
  // classes to customize
  // their behavior
  private static class Config {
    Config(
        String[] fields,
        String type,
        MultiDateTimeFormatter fromFormat,
        DateTimeFormatter toFormat,
        String unixPrecision,
        String header,
        Optional<RollingWindowDetails> rollingWindow,
        TimeZone targetTimeZone) {
      this.fields = fields;
      this.type = type;
      this.fromFormat = fromFormat;
      this.toFormat = toFormat;
      this.unixPrecision = unixPrecision;
      this.header = header;
      this.rollingWindow = rollingWindow;
      this.targetTimeZone = targetTimeZone;
      this.targetTimeZoneId = targetTimeZone.toZoneId();
    }

    String[] fields;
    String header;
    String type;
    final MultiDateTimeFormatter fromFormat;
    final DateTimeFormatter toFormat;
    String unixPrecision;

    Optional<RollingWindowDetails> rollingWindow;

    TimeZone targetTimeZone;
    final ZoneId targetTimeZoneId;
  }

  private FieldType fieldType;
  private Config config;

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
    String fieldConfig = simpleConfig.getString(FIELD_CONFIG);
    if (fieldConfig == null || fieldConfig.isEmpty()) {
      fieldConfig = FieldTypeConstants.VALUE_FIELD;
    }
    final String type = simpleConfig.getString(TARGET_TYPE_CONFIG);
    final String header = simpleConfig.getString(HEADER_NAME_CONFIG);
    if (header == null || header.isEmpty()) {
      throw new ConfigException("TimestampConverter requires header key to be specified");
    }
    List<String> fromFormatPattern = simpleConfig.getList(FORMAT_FROM_CONFIG);
    String toFormatPattern = simpleConfig.getString(FORMAT_TO_CONFIG);

    final String unixPrecision = simpleConfig.getString(UNIX_PRECISION_CONFIG);

    final String targetTimeZone = simpleConfig.getString(TARGET_TIMEZONE_CONFIG);
    TimeZone timeZone = TimeZone.getTimeZone(targetTimeZone);
    if (type.equals(TYPE_STRING) && isBlank(toFormatPattern)) {
      throw new ConfigException(
          "TimestampConverter requires format option to be specified "
              + "when using string timestamps");
    }
    MultiDateTimeFormatter fromPattern = MultiDateTimeFormatter.createDateTimeFormatter(
            fromFormatPattern, FORMAT_FROM_CONFIG, Constants.UTC.toZoneId());

    DateTimeFormatter toPattern =
        io.lenses.connect.smt.header.Utils.getDateFormat(toFormatPattern, timeZone.toZoneId());

    FieldTypeUtils.FieldTypeAndFields fieldTypeAndFields =
        FieldTypeUtils.extractFieldTypeAndFields(fieldConfig);
    fieldType = fieldTypeAndFields.getFieldType();
    // ignore NONE as a rolling window type
    final HashSet<String> ignoredRollingWindowTypes =
        new HashSet<>(Collections.singletonList("NONE"));
    Optional<RollingWindow> rollingWindow =
        RollingWindowUtils.extractRollingWindow(
            simpleConfig, ROLLING_WINDOW_TYPE_CONFIG, ignoredRollingWindowTypes);
    Optional<RollingWindowDetails> rollingWindowDetails =
        rollingWindow.flatMap(
            rw ->
                RollingWindowUtils.extractRollingWindowSize(
                        simpleConfig, rw, ROLLING_WINDOW_SIZE_CONFIG)
                    .map(size -> new RollingWindowDetails(rw, size)));
    // if rolling window is set, check that the window size is set
    if (rollingWindow.isPresent() && !rollingWindowDetails.isPresent()) {
      throw new ConfigException(
          "TimestampConverter requires a window size to be specified "
              + "when using rolling window timestamps.");
    }

    config =
        new Config(
            fieldTypeAndFields.getFields(),
            type,
            fromPattern,
            toPattern,
            unixPrecision,
            header,
            rollingWindowDetails,
            timeZone);
  }

  @Override
  public R apply(R record) {
    if (record == null) {
      return null;
    }
    SchemaAndValue result;
    if (operatingSchema(record) == null) {
      result = fromSchemaless(record);
    } else {
      result = fromRecordWithSchema(record);
    }
    if (result == null) {
      return record;
    }
    record.headers().add(config.header, new SchemaAndValue(result.schema(), result.value()));
    return record;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  private Schema operatingSchema(R record) {
    if (fieldType == FieldType.TIMESTAMP) {
      // A record timestamp is epoch time and corresponds to the Logical Type Timestamp
      return Timestamp.SCHEMA;
    }
    if (fieldType == FieldType.KEY) {
      return record.keySchema();
    }
    return record.valueSchema();
  }

  private Object operatingValue(R record) {
    if (fieldType == FieldType.TIMESTAMP) {
      return record.timestamp();
    }
    if (fieldType == FieldType.KEY) {
      return record.key();
    }
    return record.value();
  }

  private SchemaAndValue fromRecordWithSchema(R record) {

    if (config.fields.length == 0) {
      Object value = operatingValue(record);
      final Schema schema = operatingSchema(record);
      return convertTimestamp(value, timestampTypeFromSchema(schema));
    } else {
      final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
      return from(value);
    }
  }

  private SchemaAndValue from(Struct value) {
    if (value == null) {
      return null;
    }
    // config.fields contains the path to the field. Starting from value navigate to the field or if
    // not found return null

    // From 0 to config.fields.length-2 navigate extract a struct from the field
    // For the last field extract the value
    Struct updatedValue = value;

    for (int i = 0; i < config.fields.length - 1; i++) {
      updatedValue = updatedValue.getStruct(config.fields[i]);
      if (updatedValue == null) {
        return null;
      }
    }
    // updatedValue is now the struct containing the field to be updated
    // config.fields[config.fields.length-1] is the name of the field to be updated
    Field field = updatedValue.schema().field(config.fields[config.fields.length - 1]);
    if (field == null) {
      return null;
    }
    final Schema fieldSchema = field.schema();
    final Object fieldValue = updatedValue.get(config.fields[config.fields.length - 1]);
    return convertTimestamp(fieldValue, timestampTypeFromSchema(fieldSchema));
  }

  private SchemaAndValue fromSchemaless(R record) {
    Object rawValue = operatingValue(record);
    if (rawValue == null || config.fields.length == 0) {
      return convertTimestamp(rawValue);
    } else {

      // for 0 to config.fields[config.fields.length-2] navigate to the field and extract a map
      // for the last field extract the value
      Map<String, Object> updatedValue = requireMap(rawValue, PURPOSE);
      for (int i = 0; i < config.fields.length - 1; i++) {
        updatedValue = requireMap(updatedValue.get(config.fields[i]), PURPOSE);
        if (updatedValue == null) {
          return null;
        }
      }
      // updatedValue is now the map containing the field to be updated
      // config.fields[config.fields.length-1] is the name of the field to be updated
      return convertTimestamp(updatedValue.get(config.fields[config.fields.length - 1]));
    }
  }

  /** Determine the type/format of the timestamp based on the schema. */
  private String timestampTypeFromSchema(Schema schema) {
    if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
      return TYPE_TIMESTAMP;
    } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
      return TYPE_DATE;
    } else if (Time.LOGICAL_NAME.equals(schema.name())) {
      return TYPE_TIME;
    } else if (schema.type().equals(Schema.Type.STRING)) {
      // If not otherwise specified, string == user-specified string format for timestamps
      return TYPE_STRING;
    } else if (schema.type().equals(Schema.Type.INT64)) {
      // If not otherwise specified, long == unix time
      return TYPE_UNIX;
    }
    throw new ConnectException(
        "Schema " + schema + " does not correspond to a known timestamp type format.");
  }

  /** Infer the type/format of the timestamp based on the raw Java type. */
  private String inferTimestampType(Object timestamp) {
    // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime
    // representation as a
    // java.util.Date
    if (timestamp instanceof Date) {
      return TYPE_TIMESTAMP;
    } else if (timestamp instanceof Long) {
      return TYPE_UNIX;
    } else if (timestamp instanceof String) {
      // check if timestamp is Long and then return unix
      try {
        Long.parseLong((String) timestamp);
        return TYPE_UNIX;
      } catch (NumberFormatException e) {
        return TYPE_STRING;
      }
    }
    throw new DataException(
        "TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps.");
  }

  /**
   * Convert the given timestamp to the target timestamp format.
   *
   * @param timestamp the input timestamp, may be null
   * @param timestampFormat the format of the timestamp, or null if the format should be inferred
   * @return the converted timestamp
   */
  private SchemaAndValue convertTimestamp(Object timestamp, String timestampFormat) {
    TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
    if (targetTranslator == null) {
      throw new ConnectException("Unsupported timestamp type: " + config.type);
    }
    if (timestamp == null) {
      return new SchemaAndValue(targetTranslator.typeSchema(true), null);
    }
    if (timestampFormat == null) {
      timestampFormat = inferTimestampType(timestamp);
    }

    TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
    if (sourceTranslator == null) {
      throw new ConnectException("Unsupported timestamp type: " + timestampFormat + ".");
    }
    final Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);

    final Date adjustedTimestamp =
        config
            .rollingWindow
            .map(
                rw -> {
                  final Instant instant = rawTimestamp.toInstant();
                  final Instant adjusted = rw.adjust(instant);
                  return Date.from(adjusted);
                })
            .orElse(rawTimestamp);

    return new SchemaAndValue(
        targetTranslator.typeSchema(true), targetTranslator.toType(config, adjustedTimestamp));
  }

  private SchemaAndValue convertTimestamp(Object timestamp) {
    return convertTimestamp(timestamp, null);
  }
}
