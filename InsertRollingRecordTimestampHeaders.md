# Insert Rolling Record Timestamp Headers

## Description

A Kafka Connect Single Message Transform (SMT) that inserts date, year, month,day, hour, minute and second headers using
the record timestamp and a rolling time window configuration. If the record timestamp is null, the SMT uses the current
system time.

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

| Name                  | Description                                                                                                                                                   | Type   | Default    | Importance              |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------------|-------------------------|
| `header.prefix.name`  | Optional header prefix.                                                                                                                                       | String |            | Low                     |
| `date.format`         | Optional Java date time formatter.                                                                                                                            | String | yyyy-MM-dd | Low                     |
| `year.format`         | Optional Java date time formatter for the year component.                                                                                                     | String | yyyy       | Low                     |
| `month.format`        | Optional Java date time formatter for the month component.                                                                                                    | String | MM         | Low                     |
| `day.format`          | Optional Java date time formatter for the day component.                                                                                                      | String | dd         | Low                     |
| `hour.format`         | Optional Java date time formatter for the hour component.                                                                                                     | String | HH         | Low                     |
| `minute.format`       | Optional Java date time formatter for the minute component.                                                                                                   | String | mm         | Low                     |
| `second.format`       | Optional Java date time formatter for the second component.                                                                                                   | String | ss         | Low                     |
| `timezone`            | Optional. Sets the timezone. It can be any valid Java timezone.                                                                                               | String | UTC        | Low                     |
| `locale`              | Optional. Sets the locale. It can be any valid Java locale.                                                                                                   | String | en         | Low                     |
| `rolling.window.type` | Sets the window type. It can be fixed or rolling.                                                                                                             | String | minutes    | hours, minutes, seconds | High       | 
| `rolling.window.size` | Sets the window size. It can be any positive integer, and depending on the `window.type` it has an upper bound, 60 for seconds and minutes, and 24 for hours. | Int    | 15         |                         | High       |

## Example

To store the epoch value, use the following configuration:

```properties
transforms=rollingWindow
transforms.rollingWindow.type=io.lenses.connect.smt.header.InsertRollingRecordTimestampHeaders
transforms.rollingWindow.rolling.window.type=minutes
transforms.rollingWindow.rolling.window.size=15
```

To prefix the headers with `wallclock_`, use the following:

```properties
transforms=rollingWindow
transforms.rollingWindow.type=io.lenses.connect.smt.header.InsertRollingRecordTimestampHeaders
transforms.rollingWindow.header.prefix.name=wallclock_
transforms.rollingWindow.rolling.window.type=minutes
transforms.rollingWindow.rolling.window.size=15
```

To change the date format, use the following:

```properties
transforms=rollingWindow
transforms.rollingWindow.type=io.lenses.connect.smt.header.InsertRollingRecordTimestampHeaders
transforms.rollingWindow.header.prefix.name=wallclock_
transforms.rollingWindow.rolling.window.type=minutes
transforms.rollingWindow.rolling.window.size=15
transforms.rollingWindow.date.format="date=yyyy-MM-dd"
```

To use the timezone `Asia/Kolkoata`, use the following:

```properties
transforms=rollingWindow
transforms.rollingWindow.type=io.lenses.connect.smt.header.InsertRollingRecordTimestampHeaders
transforms.rollingWindow.header.prefix.name=wallclock_
transforms.rollingWindow.rolling.window.type=minutes
transforms.rollingWindow.rolling.window.size=15
transforms.rollingWindow.timezone=Asia/Kolkata
```


To facilitate S3, GCS, or Azure Data Lake partitioning using a Hive-like partition name format, such
as `date=yyyy-MM-dd / hour=HH`, employ the following SMT configuration for a partition strategy.

```properties
transforms=rollingWindow
transforms.rollingWindow.type=io.lenses.connect.smt.header.InsertRollingRecordTimestampHeaders
transforms.rollingWindow.rolling.window.type=minutes
transforms.rollingWindow.rolling.window.size=15
transforms.rollingWindow.timezone=Asia/Kolkata
transforms.rollingWindow.date.format="date=yyyy-MM-dd"
transforms.rollingWindow.hour.format="hour=yyyy"
```

and in the KCQL setting utilise the headers as partitioning keys:

```properties
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY _header.date, _header.year
```