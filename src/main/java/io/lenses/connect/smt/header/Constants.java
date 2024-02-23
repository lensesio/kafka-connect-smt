package io.lenses.connect.smt.header;

import java.time.ZoneId;
import java.util.TimeZone;

public class Constants {
  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  public static final ZoneId UTC_ZONE_ID = UTC.toZoneId();
}
