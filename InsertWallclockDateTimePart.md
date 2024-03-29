# Insert Wallclock DateTime Part Header

## Description

> **Note:** Use [InsertWallclockHeaders](./InsertWallclockHeaders.md) SMT if you want to use more than one date time
> part. This avoids multiple SMTs and is more efficient.
>
A Kafka Connect Single Message Transform (SMT) that inserts the system clock year, month, day, minute, or seconds as a
message header, with a value of type STRING.

## Configuration

| Name             | Description                                           | Type   | Default | Valid Values                          | Importance |
|------------------|-------------------------------------------------------|--------|---------|---------------------------------------|------------|
| `header.name`    | The name of the header to insert the timestamp into.  | String |         |                                       | High       |
| `date.time.part` | The date time part to insert.                         | String |         | year, month, day, hour,minute, second | High       |
| `timezone`       | Sets the timezone. It can be any valid java timezone. | String | UTC     |                                       | High       |

## Example

To store the year, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=year
```

To store the month, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=month
```

To store the day, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=day
```

To store the hour, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=hour
```

To store the hour, and apply a timezone, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=hour
transforms.InsertWallclockDateTimePart.timezone=Asia/Kolkata
```

To store the minute, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=minute
```

To store the second, use the following configuration:

```properties
transforms=InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.type=io.lenses.connect.smt.header.InsertWallclockDateTimePart
transforms.InsertWallclockDateTimePart.header.name=wallclock
transforms.InsertWallclockDateTimePart.date.time.part=second
```