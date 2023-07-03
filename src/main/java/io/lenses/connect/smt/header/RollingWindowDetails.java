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
