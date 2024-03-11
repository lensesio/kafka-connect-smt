# Insert Wallclock

## Description

A Kafka Connect Single Message Transform (SMT) that inserts date, year, month,day, hour, minute and second headers using
the record timestamp. If the record timestamp is null, the SMT uses the current system time.

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
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY _date, _hour
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY _year, _month, _day, _hour
```

## Configuration

| Name                 | Description                                                     | Type   | Default    | Importance |
|----------------------|-----------------------------------------------------------------|--------|------------|------------|
| `header.prefix.name` | Optional header prefix.                                         | String |            | Low        |
| `date.format`        | Optional Java date time formatter.                              | String | yyyy-MM-dd | Low        |
| `year.format`        | Optional Java date time formatter for the year component.       | String | yyyy       | Low        |
| `month.format`       | Optional Java date time formatter for the month component.      | String | MM         | Low        |
| `day.format`         | Optional Java date time formatter for the day component.        | String | dd         | Low        |
| `hour.format`        | Optional Java date time formatter for the hour component.       | String | HH         | Low        |
| `minute.format`      | Optional Java date time formatter for the minute component.     | String | mm         | Low        |
| `second.format`      | Optional Java date time formatter for the second component.     | String | ss         | Low        |
| `timezone`           | Optional. Sets the timezone. It can be any valid Java timezone. | String | UTC        | Low        |
| `locale`             | Optional. Sets the locale. It can be any valid Java locale.     | String | en         | Low        |

## Example

To store the epoch value, use the following configuration:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclockHeaders
```

To prefix the headers with `wallclock_`, use the following:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclockHeaders
transforms.InsertWallclock.header.prefix.name=wallclock_
```

To change the date format, use the following:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclockHeaders
transforms.InsertWallclock.date.format=yyyy-MM-dd
```

To use the timezone `Asia/Kolkoata`, use the following:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclockHeaders
transforms.InsertWallclock.timezone=Asia/Kolkata
```

To facilitate S3, GCS, or Azure Data Lake partitioning using a Hive-like partition name format, such
as `date=yyyy-MM-dd / hour=HH`, employ the following SMT configuration for a partition strategy.

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclockHeaders    
transforms.InsertWallclock.date.format="date=yyyy-MM-dd"
transforms.InsertWallclock.hour.format="hour=yyyy"
```

and in the KCQL setting utilise the headers as partitioning keys:

```properties
connect.s3.kcql=INSERT INTO $bucket:prefix SELECT * FROM kafka_topic PARTITIONBY date, year
```