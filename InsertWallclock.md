# Insert Wallclock

## Description

> **Note:** Use [InsertWallclockHeaders](./InsertWallclockHeaders.md) SMT if you want to use more than one date time
> part. This avoids multiple SMTs and is more efficient. For example if you want to partition the data
> by `yyyy-MM-dd/HH`,
> then you can use `InsertWallclockHeaders` which inserts multiple headers: date, year, month,day, hour, minute, second.
>
A Kafka Connect Single Message Transform (SMT) that inserts the system clock as a message header.

Inserts the system clock as a message header, with a value of type STRING. The value can be either a string
representation of the date and time epoch value, or a string representation of the date and time in the format specified
for example `yyyy-MM-dd HH:mm:ss.SSS`.

## Configuration

| Name          | Description                                                                                                                                    | Type   | Default | Valid Values | Importance |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------|--------|---------|--------------|------------|
| `header.name` | The name of the header to insert the timestamp into.                                                                                           | String |         |              | High       |
| `value.type`  | Sets the header value inserted. It can be epoch or string. If string is used, then the 'format' setting is required."                          | String | format  | epoch,format | High       |
| `format`      | Sets the format of the header value inserted if the type was set to string. It can be any valid java date format.                              | String |         |              | High       |
| `timezone`    | Sets the timezone. It can be any valid java timezone. Overwrite it when `value.type` is set to `format`, otherwise it will raise an exception. | String | UTC     |              | High       |

## Example

To store the epoch value, use the following configuration:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclock
transforms.InsertWallclock.header.name=wallclock
transforms.InsertWallclock.value.type=epoch
```

To store a string representation of the date and time in the format `yyyy-MM-dd HH:mm:ss.SSS`, use the following:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclock
transforms.InsertWallclock.header.name=wallclock
transforms.InsertWallclock.value.type=format
transforms.InsertWallclock.format=yyyy-MM-dd HH:mm:ss.SSS
```

To use the timezone `Asia/Kolkoata`, use the following:

```properties
transforms=InsertWallclock
transforms.InsertWallclock.type=io.lenses.connect.smt.header.InsertWallclock
transforms.InsertWallclock.header.name=wallclock
transforms.InsertWallclock.value.type=format
transforms.InsertWallclock.format=yyyy-MM-dd HH:mm:ss.SSS
transforms.InsertWallclock.timezone=Asia/Kolkata
```