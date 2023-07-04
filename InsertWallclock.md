# Insert Wallclock

## Description

A Kafka Connect Single Message Transform (SMT) that inserts the system clock as a message header.

Inserts the system clock as a message header, with a value of type STRING. The value can be either a string
representation of the date and time epoch value, or a string representation of the date and time in the format specified
for example `yyyy-MM-dd HH:mm:ss.SSS`.

## Configuration

| Name          | Description                                                                                                           | Type   | Default | Valid Values | Importance |
|---------------|-----------------------------------------------------------------------------------------------------------------------|--------|---------|--------------|------------|
| `header.name` | The name of the header to insert the timestamp into.                                                                  | String |         |              | High       |
| `value.type`  | Sets the header value inserted. It can be epoch or string. If string is used, then the 'format' setting is required." | String | format  | epoch,format | High       |
| `format`      | Sets the format of the header value inserted if the type was set to string. It can be any valid java date format.     | String |         |              | High       |



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
transforms.InsertWallclock.value.type=string
transforms.InsertWallclock.format=yyyy-MM-dd HH:mm:ss.SSS
```