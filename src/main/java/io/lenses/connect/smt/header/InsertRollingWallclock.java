package io.lenses.connect.smt.header;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A transformation that inserts a header with the current wallclock time bound by a time window.
 * For example, the requirement is to capture rolling data every 15 minutes, or 5 hours. This
 * implementation is less costly than the InsertField SMT where the payload is modified leading to
 * more memory used a more CPU time.
 */
public class InsertRollingWallclock<R extends ConnectRecord<R>> implements Transformation<R> {

  private String headerKey;
  private DateTimeFormatter format;

  private int rollingWindowValue;
  private RollingWindow rollingWindow;
  private Supplier<Instant> instantF = Instant::now;

  private Supplier<String> valueExtractorF;

  // Rolling window set to 15minutes by default
  private static final int DEFAULT_ROLLING_WINDOW_VALUE = 15;
  private static final RollingWindow DEFAULT_ROLLING_WINDOW = RollingWindow.MINUTES;
  private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  private interface ConfigName {
    String HEADER_NAME = "header.name";
    String ROLLING_WINDOW = "window.type";
    String FORMAT = "format";
    String VALUE_TYPE = "value.type";

    String ROLLING_WINDOW_VALUE = "window.size";

    String VALUE_TYPE_EPOCH = "epoch";
    String VALUE_TYPE_FORMAT = "format";
  }

  enum RollingWindow {
    HOURS,
    MINUTES,
    SECONDS
  }

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ConfigName.HEADER_NAME,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "The header name")
          .define(
              ConfigName.ROLLING_WINDOW,
              ConfigDef.Type.STRING,
              DEFAULT_ROLLING_WINDOW.name(),
              ConfigDef.Importance.HIGH,
              "The rolling window")
          .define(
              ConfigName.FORMAT,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "The format of the date")
          .define(
              ConfigName.VALUE_TYPE,
              ConfigDef.Type.STRING,
              "format",
              ConfigDef.Importance.HIGH,
              "The type of the value, either epoch or format")
          .define(
              ConfigName.ROLLING_WINDOW_VALUE,
              ConfigDef.Type.INT,
              DEFAULT_ROLLING_WINDOW_VALUE,
              ConfigDef.Importance.HIGH,
              "The rolling window value");

  /**
   * Used to testing only to inject the instant value.
   *
   * @param instantF - the instant supplier
   */
  void setInstantF(Supplier<Instant> instantF) {
    this.instantF = instantF;
  }

  @Override
  public R apply(R r) {
    if (r == null) {
      return null;
    }
    String value = valueExtractorF.get();
    r.headers().addString(headerKey, value);
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
    headerKey = config.getString(ConfigName.HEADER_NAME);
    if (headerKey == null) {
      throw new ConfigException("Configuration '" + ConfigName.HEADER_NAME + "' must be set.");
    } else if (headerKey.isEmpty()) {
      throw new ConfigException(
          "Configuration '" + ConfigName.HEADER_NAME + "' must not be empty.");
    }
    String valueType = config.getString(ConfigName.VALUE_TYPE);
    if (valueType == null) {
      throw new ConfigException("Configuration '" + ConfigName.VALUE_TYPE + "' must be set.");
    } else if (valueType.isEmpty()) {
      throw new ConfigException("Configuration '" + ConfigName.VALUE_TYPE + "' must not be empty.");
    } else if (!valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_EPOCH)
        && !valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_FORMAT)) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.VALUE_TYPE
              + "' must be set to either 'epoch' or 'format'.");
    }
    if (valueType.equalsIgnoreCase(ConfigName.VALUE_TYPE_FORMAT)) {
      final String pattern = config.getString(ConfigName.FORMAT);
      if (pattern == null) {
        format = DEFAULT_FORMATTER;
      } else {
        try {
          format = DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException e) {
          throw new ConfigException(
              "Configuration '" + ConfigName.FORMAT + "' is not a valid date format.", e);
        }
      }
      valueExtractorF = this::getFormattedValue;
    } else {
      valueExtractorF = this::getEpochValue;
    }

    // extract the rolling window and rolling window value
    String rollingWindow = config.getString(ConfigName.ROLLING_WINDOW);
    if (rollingWindow == null) {
      throw new ConfigException("Configuration '" + ConfigName.ROLLING_WINDOW + "' must be set.");
    } else {
      try {
        this.rollingWindow = RollingWindow.valueOf(rollingWindow.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
            "Configuration '" + ConfigName.ROLLING_WINDOW + "' is not a valid rolling window.");
      }
    }

    rollingWindowValue = config.getInt(ConfigName.ROLLING_WINDOW_VALUE);
    // validate the value to be positive int and if
    // rolling window is minutes it cannot be more than 60
    // rolling window is hours it cannot be more than 24
    // rolling window is seconds it cannot be more than 60
    if (rollingWindowValue <= 0) {
      throw new ConfigException(
          "Configuration '" + ConfigName.ROLLING_WINDOW_VALUE + "' must be a positive integer.");
    } else if (this.rollingWindow == RollingWindow.MINUTES && rollingWindowValue > 60) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.ROLLING_WINDOW_VALUE
              + "' must be less than or equal to 60.");
    } else if (this.rollingWindow == RollingWindow.HOURS && rollingWindowValue > 24) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.ROLLING_WINDOW_VALUE
              + "' must be less than or equal to 24.");
    } else if (this.rollingWindow == RollingWindow.SECONDS && rollingWindowValue > 60) {
      throw new ConfigException(
          "Configuration '"
              + ConfigName.ROLLING_WINDOW_VALUE
              + "' must be less than or equal to 60.");
    }
  }

  /**
   * Adjust the instant based on the rolling window type and rolling window value. Considering that
   * the instant is in UTC, the adjusted instant will be truncated to the nearest window start. For
   * example if the rolling window is minutes and the value is 15, then all instants where minute is
   * [0,15) will be adjusted to the start of the minute 0, for [15, 30) will be adjusted to the
   * start of the minute 15, for [30, 45) will be adjusted to the start of the minute 30 and for
   * [45, 60) will be adjusted to the start of the minute 45, and for [45, 60) will be adjusted to
   * the start of the minute 45.
   *
   * @return - the adjusted instant
   */
  private Instant getAdjustedInstant() {
    final Instant wallclock = instantF.get();
    Instant windowStart = wallclock;

    // There is a discrete set of time windows, each window has a start time and a duration -
    // defined by rolling window value
    // The start time of the window is the time instant that is the start of the window.
    // Any instant that falls within the window will be adjusted to the start of the window.

    switch (rollingWindow) {
      case MINUTES:
        windowStart =
            wallclock
                .minusSeconds(wallclock.getEpochSecond() % (rollingWindowValue * 60))
                .truncatedTo(ChronoUnit.MINUTES);
        break;
      case HOURS:
        windowStart =
            wallclock
                .minusSeconds(wallclock.getEpochSecond() % (rollingWindowValue * 60 * 60))
                .truncatedTo(ChronoUnit.HOURS);
        break;
      case SECONDS:
        windowStart =
            wallclock
                .minusSeconds(wallclock.getEpochSecond() % rollingWindowValue)
                .truncatedTo(ChronoUnit.SECONDS);
        break;

      default:
        throw new ConfigException("Invalid rolling window type");
    }
    return windowStart;
  }

  private String getEpochValue() {
    Instant wallclock = getAdjustedInstant();
    return String.valueOf(wallclock.toEpochMilli());
  }

  private String getFormattedValue() {
    Instant wallclock = getAdjustedInstant();

    OffsetDateTime dateTime = OffsetDateTime.ofInstant(wallclock, ZoneOffset.UTC);
    return format.format(dateTime);
  }
}
