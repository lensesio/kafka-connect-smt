# Timestamp Converter

## Description

An adapted version of the [TimestampConverter](https://github.com/apache/kafka/blob/5c2492bca71200806ccf776ea31639a90290d43e/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampConverter.java#L50) SMT, that allows the user to specify the format of the timestamp inserted as a header.
It also avoids the synchronization block requirement for converting to a string representation of the timestamp.

The SMT adds a few more features to the original:

* allows nested fields resolution (e.g. `a.b.c`)
* uses _key or _value as prefix to understand the field to convert is part of the record Key or Value
* allows conversion from one string representation to another (e.g. `yyyy-MM-dd HH:mm:ss` to `yyyy-MM-dd`)
* allows conversion using a rolling window boundary (e.g. every 15 minutes, or one hour)


## Configuration


| Name                  | Description                                                                                                                                                                                                                                                                                                                          | Type   | Default      | Valid Values                                     |  
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|--------------|--------------------------------------------------| 
| `header.name`         | The name of the header to insert the timestamp into.                                                                                                                                                                                                                                                                                 | String |              |                                                  |
| `field`               | The field path containing the timestamp, or empty if the entire value is a timestamp. Prefix the path with the literal string `_key` or `_value` to specify the record Key or Value is used as source. If not specified `_value` is implied.                                                                                         | String |              |                                                  |
| `target.type`         | Sets the desired timestamp representation.                                                                                                                                                                                                                                                                                           | String |              | string,unix,date,time,timestamp                  |
| `format.from.pattern` | Sets the format of the timestamp when the input is a string. The format requires a Java DateTimeFormatter-compatible pattern.                                                                                                                                                                                                        | String |              |                                                  |
| `format.to.pattern`   | Sets the format of the timestamp when the output is a string. The format requires a Java DateTimeFormatter-compatible pattern.                                                                                                                                                                                                       | String |              |                                                  |
| `rolling.window.type` | An optional parameter for the rolling time window type. When set it will adjust the output value according to the time window boundary.                                                                                                                                                                                              | String | none         | none, hours, minutes, seconds                    |
| `rolling.window.size` | An optional positive integer parameter for the rolling time window size. When `rolling.window.type` is defined this setting is required. The value is bound by the `rolling.window.type` configuration. If type is `minutes` or `seconds` then the value cannot bigger than 60, and if the type is `hours` then the max value is 24. | Int    | 15           |                                                  |
| `unix.precision`      | The desired Unix precision for the timestamp. Used to generate the output when type=unix or used to parse the input if the input is a Long. This SMT will cause precision loss during conversions from, and to, values with sub-millisecond components.                                                                              | String | milliseconds | seconds, milliseconds, microseconds, nanoseconds |


## Example

To convert to and from a string representation of the date and time in the format `yyyy-MM-dd HH:mm:ss.SSS`, use the following configuration:

```properties
transforms=TimestampConverter
transforms.TimestampConverter.type=io.lenses.connect.smt.header.TimestampConverter
transforms.TimestampConverter.header.name=wallclock
transforms.TimestampConverter.field=_value.ts
transforms.TimestampConverter.target.type=string
transforms.TimestampConverter.format.from.pattern=yyyyMMddHHmmssSSS
transforms.TimestampConverter.format.to.pattern=yyyy-MM-dd HH:mm:ss.SSS
```

To convert to and from a string representation while applying an hourly rolling window:


```properties
transforms=TimestampConverter
transforms.TimestampConverter.type=io.lenses.connect.smt.header.TimestampConverter
transforms.TimestampConverter.header.name=wallclock
transforms.TimestampConverter.field=_value.ts
transforms.TimestampConverter.target.type=string
transforms.TimestampConverter.format.from.pattern=yyyyMMddHHmmssSSS
transforms.TimestampConverter.format.to.pattern=yyyy-MM-dd-HH
transforms.TimestampConverter.rolling.window.type=hours
transforms.TimestampConverter.rolling.window.size=1
```

To convert to and from a string representation while applying a 15 minutes rolling window:


```properties
transforms=TimestampConverter
transforms.TimestampConverter.type=io.lenses.connect.smt.header.TimestampConverter
transforms.TimestampConverter.header.name=wallclock
transforms.TimestampConverter.field=_value.ts
transforms.TimestampConverter.target.type=string
transforms.TimestampConverter.format.from.pattern=yyyyMMddHHmmssSSS
transforms.TimestampConverter.format.to.pattern=yyyy-MM-dd-HH-mm
transforms.TimestampConverter.rolling.window.type=minutes
transforms.TimestampConverter.rolling.window.size=15
```


To convert to and from a Unix timestamp, use the following:

```properties
transforms=TimestampConverter
transforms.TimestampConverter.type=io.lenses.connect.smt.header.TimestampConverter
transforms.TimestampConverter.header.name=wallclock
transforms.TimestampConverter.field=_key.ts
transforms.TimestampConverter.target.type=unix
transforms.TimestampConverter.unix.precision=milliseconds
```