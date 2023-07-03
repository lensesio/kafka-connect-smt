package io.lenses.connect.smt.header;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/** Helper class to extract rolling window configuration from the supplied config. */
class RollingWindowUtils {
  static Optional<RollingWindow> extractRollingWindow(
      SimpleConfig config, String configName, Set<String> ignoredValues) {
    String rollingWindowType = config.getString(configName);
    if (rollingWindowType == null
        || rollingWindowType.isEmpty()
        || ignoredValues.contains(rollingWindowType.toUpperCase(Locale.ROOT))) {
      return Optional.empty();
    }
    try {
      RollingWindow rw = RollingWindow.valueOf(rollingWindowType.toUpperCase(Locale.ROOT));
      return Optional.of(rw);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          "Configuration '" + configName + "' must be one of [minutes, hours, seconds].");
    }
  }

  static Optional<Integer> extractRollingWindowSize(
      SimpleConfig config, RollingWindow rollingWindow, String configName) {
    Integer rollingWindowSize = config.getInt(configName);
    if (rollingWindowSize == null) {
      return Optional.empty();
    }
    // validate the value to be positive int and if
    // rolling window is minutes it cannot be more than 60
    // rolling window is hours it cannot be more than 24
    // rolling window is seconds it cannot be more than 60
    if (rollingWindowSize <= 0) {
      throw new ConfigException("Configuration '" + configName + "' must be a positive integer.");
    } else if (rollingWindow == RollingWindow.MINUTES && rollingWindowSize > 60) {
      throw new ConfigException(
          "Configuration '" + configName + "' must be less than or equal to 60.");
    } else if (rollingWindow == RollingWindow.HOURS && rollingWindowSize > 24) {
      throw new ConfigException(
          "Configuration '" + configName + "' must be less than or equal to 24.");
    } else if (rollingWindow == RollingWindow.SECONDS && rollingWindowSize > 60) {
      throw new ConfigException(
          "Configuration '" + configName + "' must be less than or equal to 60.");
    }
    return Optional.of(rollingWindowSize);
  }
}
