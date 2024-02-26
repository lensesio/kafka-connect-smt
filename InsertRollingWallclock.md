# Insert Rolling Wallclock

## Description

An SMT that inserts the system clock value as a message header, a value adapted to a specified  time window boundary, for example every 15 minutes, or one hour.
The value inserted is stored as a STRING, and it holds either a string representation of the date and time epoch value, or a string representation of the date and time in the format specified.


## Configuration


| Name                  | Description                                                                                                                                                   | Type   | Default | Valid Values            | Importance |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|---------|-------------------------|------------|
| `header.name`         | The name of the header to insert the timestamp into.                                                                                                          | String |         |                         | High       |
| `value.type`          | Sets the header value inserted. It can be epoch or string. If string is used, then the 'format' setting is required."                                         | String | format  | epoch,format            | High       |
| `format`              | Sets the format of the header value inserted if the type was set to string. It can be any valid java date format.                                             | String |         |                         | High       |
| `rolling.window.type` | Sets the window type. It can be fixed or rolling.                                                                                                             | String | minutes | hours, minutes, seconds | High       | 
| `rolling.window.size` | Sets the window size. It can be any positive integer, and depending on the `window.type` it has an upper bound, 60 for seconds and minutes, and 24 for hours. | Int    | 15      |                         | High       |
| `timezone`            | Sets the timezone. It can be any valid java timezone. Overwrite it when `value.type` is set to `format`, otherwise it will raise an exception.                | String | UTC     |                         | High       |

## Example

To store the epoch value, use the following configuration:

```properties
transforms=InsertRollingWallclock
transforms.InsertRollingWallclock.type=io.lenses.connect.smt.header.InsertRollingWallclock
transforms.InsertRollingWallclock.header.name=wallclock
transforms.InsertRollingWallclock.value.type=epoch
transforms.InsertRollingWallclock.rolling.window.type=minutes
transforms.InsertRollingWallclock.rolling.window.size=15
```

To store a string representation of the date and time in the format `yyyy-MM-dd HH:mm:ss.SSS`, use the following:

```properties
transforms=InsertRollingWallclock
transforms.InsertRollingWallclock.type=io.lenses.connect.smt.header.InsertRollingWallclock
transforms.InsertRollingWallclock.header.name=wallclock
transforms.InsertRollingWallclock.value.type=format
transforms.InsertRollingWallclock.format=yyyy-MM-dd HH:mm:ss.SSS
transforms.InsertRollingWallclock.rolling.window.type=minutes
transforms.InsertRollingWallclock.rolling.window.size=15
```

To use the timezone `Asia/Kolkoata`, use the following:

```properties
transforms=InsertRollingWallclock
transforms.InsertRollingWallclock.type=io.lenses.connect.smt.header.InsertRollingWallclock
transforms.InsertRollingWallclock.header.name=wallclock
transforms.InsertRollingWallclock.value.type=format
transforms.InsertRollingWallclock.format=yyyy-MM-dd HH:mm:ss.SSS
transforms.InsertRollingWallclock.rolling.window.type=minutes
transforms.InsertRollingWallclock.rolling.window.size=15
transforms.InsertRollingWallclock.timezone=Asia/Kolkata
```