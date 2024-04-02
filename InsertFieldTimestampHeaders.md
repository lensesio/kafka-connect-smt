# Insert Field Timestamp Headers

## Description

Got it, here's the revised text with just the paragraph:

---

This Kafka Connect Single Message Transform (SMT) facilitates the insertion of date and time components (year, month,
day, hour, minute, second) as headers into Kafka messages using a timestamp field within the message payload. The
timestamp field can be in various valid formats, including long integers, strings, or date objects. The timestamp field
can originate from either the record Key or the record Value. When extracting from the record Key, prefix the field
with `_key.`; otherwise, extract from the record Value by default or explicitly using the field without prefixing. For
string-formatted fields, specify a `format.from.pattern` parameter to define the parsing pattern. Long integer fields
are assumed to be Unix timestamps; the desired Unix precision can be specified using the `unix.precision` parameter.

The headers inserted are of type STRING. By using this SMT, you can partition the data by `yyyy-MM-dd/HH`
or `yyyy/MM/dd/HH`, for example, and only use one SMT.

The list of headers inserted are:

* date
* year
* month
* day
* hour
* minute
* second

All headers can be prefixed with a custom prefix. For example, if the prefix is `wallclock_`, then the headers will be:

* wallclock_date
* wallclock_year
* wallclock_month
* wallclock_day
* wallclock_hour
* wallclock_minute
* wallclock_second

When used with the Lenses connectors for S3, GCS or Azure data lake, the headers can be used to partition the data.
Considering the headers have been prefixed by `_`, here are a few KCQL examples:

```
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY _header._date, _header._hour
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY _header._year, _header._month, _header._day, _header._hour
```

## Configuration

| Name                  | Description                                                                                                                                                                         | Type   | Default      |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|--------------|
| `field`               | The field name. If the key is part of the record Key prefix with `_key` otherwise `_value`. If `_value` or `_key` is not used it defaults to the record Value to resolve the field. | String |              |
| `format.from.pattern` | Optional date timeFormatter-compatible format for the timestamp. Used to parse the input if the input is a string.                                                                  | String |              |
| `unix.precision`      | Optional "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. Used to parse the input if the input is a Long.                        | String | milliseconds |
| `header.prefix.name`  | Optional header prefix.                                                                                                                                                             | String |              |
| `date.format`         | Optional Java date time formatter.                                                                                                                                                  | String | yyyy-MM-dd   |
| `year.format`         | Optional Java date time formatter for the year component.                                                                                                                           | String | yyyy         |
| `month.format`        | Optional Java date time formatter for the month component.                                                                                                                          | String | MM           |
| `day.format`          | Optional Java date time formatter for the day component.                                                                                                                            | String | dd           |
| `hour.format`         | Optional Java date time formatter for the hour component.                                                                                                                           | String | HH           |
| `minute.format`       | Optional Java date time formatter for the minute component.                                                                                                                         | String | mm           |
| `second.format`       | Optional Java date time formatter for the second component.                                                                                                                         | String | ss           |
| `timezone`            | Optional. Sets the timezone. It can be any valid Java timezone.                                                                                                                     | String | UTC          |
| `locale`              | Optional. Sets the locale. It can be any valid Java locale.                                                                                                                         | String | en           |

## Example

To use the record Value field named `created_at` as the unix timestamp, use the following:

```properties
transforms=fieldTs
field=_value.created_at
transforms.fieldTs.type=io.lenses.connect.smt.header.InsertFieldTimestampHeaders
```

To use the record Key field named `created_at` as the unix timestamp, use the following:

```properties
transforms=fieldTs
field=_key.created_at
transforms.fieldTs.type=io.lenses.connect.smt.header.InsertFieldTimestampHeaders
```

To prefix the headers with `wallclock_`, use the following:

```properties
transforms=fieldTs
field=created_at
transforms.fieldTs.type=io.lenses.connect.smt.header.InsertFieldTimestampHeaders
transforms.fieldTs.header.prefix.name=wallclock_
```

To change the date format, use the following:

```properties
transforms=fieldTs
field=created_at
transforms.fieldTs.type=io.lenses.connect.smt.header.InsertFieldTimestampHeader
transforms.fieldTs.date.format=yyyy-MM-dd
```

To use the timezone `Asia/Kolkoata`, use the following:

```properties
transforms=fieldTs
field=created_at
transforms.fieldTs.type=io.lenses.connect.smt.header.InsertFieldTimestampHeader
transforms.fieldTs.timezone=Asia/Kolkata
```

To facilitate S3, GCS, or Azure Data Lake partitioning using a Hive-like partition name format, such
as `date=yyyy-MM-dd / hour=HH`, employ the following SMT configuration for a partition strategy.

```properties
transforms=fieldTs
field=created_at
transforms.fieldTs.type=io.lenses.connect.smt.header.InsertFieldTimestampHeader    
transforms.fieldTs.date.format="date=yyyy-MM-dd"
transforms.fieldTs.hour.format="hour=yyyy"
```

and in the KCQL setting utilise the headers as partitioning keys:

```properties
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY _header.date, _header.year
```
