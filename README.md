# A collection of Kafka Connect Single Message Transforms (SMTs)

These SMTs are designed to be used with the [Kafka Connect](https://kafka.apache.org/documentation/#connect) framework.
The SMTs create record headers. The advantage of using headers is that they reduce the memory and CPU cycles required to change the payload. See for example the Kafka Connect [TimestampConverter](https://github.com/apache/kafka/blob/5c2492bca71200806ccf776ea31639a90290d43e/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampConverter.java#L50).
Furthermore, they support [Stream-Reactor](https://github.com/lensesio/stream-reactor) S3 sink partitioner, for scenarios like:

* Partitioning by system clock (e.g. using the system clock as a partition key with a yyyy-MM-dd-HH format)
* Partitioning by a rolling window (e.g. every 15 minutes, or one hour)
* Partitioning by a custom timestamp (e.g. a timestamp field in the payload, record Key or Value)
* Partitioning by a custom timestamp with a rolling window (e.g. a timestamp field in the payload, every 15 minutes, or one hour)

## SMTs

* [InsertWallclock](./InsertWallclock.md) - Inserts the system clock as a message header.
* [InsertRollingWallclock](./InsertRollingWallclock.md) - Inserts the system clock as a message header based on a rolling window boundary.
* [InsertRollingRecordTimestampHeaders](./InsertRollingRecordTimestampHeaders.md) - Inserts date, year, month, day, hour, minute, and second headers using the record timestamp and a rolling time window configuration.
* [InsertRollingWallclockHeaders](./InsertRollingWallclockHeaders.md) - Inserts date, year, month, day, hour, minute, and second headers using the system timestamp and a rolling time window configuration.
* [InsertRecordTimestampHeaders](./InsertRecordTimestampHeaders.md) - Inserts date, year, month, day, hour, minute, and second headers using the record timestamp.
* [InsertFieldTimestampHeaders](./InsertFieldTimestampHeaders.md) - Inserts date, year, month, day, hour, minute, and second headers using a field in the payload, record Key or Value.
* [InsertRollingFieldTimestampHeaders](./InsertRollingFieldTimestampHeaders.md) - Inserts date, year, month, day, hour, minute, and second headers using a field in the payload, record Key or Value and a rolling window boundary.
* [InsertWallclockHeaders](./InsertWallclockHeaders.md) - Inserts date, year, month, day, hour, minute, and second headers using the system clock.
* [TimestampConverter](./TimestampConverter.md) - Converts a timestamp field in the payload, record Key or Value to a different format, and optionally applies a rolling window boundary. An adapted version of the one packed in the Kafka Connect framework.
* [InsertWallclockDateTimePart](./InsertWallclockDateTimePart.md) - Inserts the system clock year, month, day, minute, or seconds as a message header, with a value of type STRING.

## Installation

The build jar can be found in the [releases](https://github.com/lensesio/kafka-connect-smt/releases). To install the jar, copy it to the `plugin.path` directory of your Kafka Connect worker.

## Build

To build the project run:

```bash
mvn clean package
```

## Checkstyle

To check the code style run:

```bash
mvn checkstyle:check
```

To format the code run:

```bash
  mvn com.coveo:fmt-maven-plugin:format
```

To add license header, run:

```bash
mvn license:format
```