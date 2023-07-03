package io.lenses.connect.smt.header;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** A representation of the rolling window configuration. */
class RollingWindowDetails {
  private final RollingWindow rollingWindow;
  private final long rollingWindowSize;

  public RollingWindowDetails(RollingWindow rollingWindow, int rollingWindowSize) {
    this.rollingWindow = rollingWindow;
    this.rollingWindowSize = rollingWindowSize;
  }

  public RollingWindow getRollingWindow() {
    return rollingWindow;
  }

  public long getRollingWindowSize() {
    return rollingWindowSize;
  }

  public Instant adjust(Instant wallclock) {
    Instant adjusted = wallclock;

    // There is a discrete set of time windows, each window has a start time and a duration -
    // defined by rolling window value
    // The start time of the window is the time instant that is the start of the window.
    // Any instant that falls within the window will be adjusted to the start of the window.

    switch (rollingWindow) {
      case MINUTES:
        adjusted =
            wallclock
                .minusSeconds(wallclock.getEpochSecond() % (rollingWindowSize * 60))
                .truncatedTo(ChronoUnit.MINUTES);
        break;
      case HOURS:
        adjusted =
            wallclock
                .minusSeconds(wallclock.getEpochSecond() % (rollingWindowSize * 60 * 60))
                .truncatedTo(ChronoUnit.HOURS);
        break;
      case SECONDS:
        adjusted =
            wallclock
                .minusSeconds(wallclock.getEpochSecond() % rollingWindowSize)
                .truncatedTo(ChronoUnit.SECONDS);
        break;

      default:
    }
    return adjusted;
  }
}
