package io.lenses.connect.smt.header;

import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import org.apache.kafka.common.config.ConfigException;

class Utils {
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  static DateTimeFormatter getDateFormat(String formatPattern) {
    if (formatPattern == null) {
      return null;
    }
    DateTimeFormatter format = null;
    if (!org.apache.kafka.common.utils.Utils.isBlank(formatPattern)) {
      try {
        format = DateTimeFormatter.ofPattern(formatPattern).withZone(UTC.toZoneId());
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
            "TimestampConverter requires a SimpleDateFormat-compatible pattern "
                + "for string timestamps: "
                + formatPattern,
            e);
      }
    }
    return format;
  }
}
