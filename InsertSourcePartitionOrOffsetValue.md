# InsertSourcePartitionOrOffsetValue

## Description

The `InsertSourcePartitionOrOffsetValue` transformation in Kafka Connect allows you to insert headers into SourceRecords based on partition or offset values. This is useful for adding metadata to your data records before they are sent to destinations like AWS S3, Azure Datalake, or GCP Storage.

## Note

This SMT only works with source connectors.

## Configuration

To use this transformation, you need to configure it in your Kafka Connect connector properties.

### Configuration Properties

| Configuration Property | Description                                                   | Optionality | Default Value  |
|------------------------|---------------------------------------------------------------|-------------|----------------|
| `offset.fields`        | Comma-separated list of fields to retrieve from the offset    | Optional    | Empty list     |
| `offset.prefix`        | Optional prefix for offset keys                               | Optional    | `"offset."`    |
| `partition.fields`     | Comma-separated list of fields to retrieve from the partition | Required    | Empty list     |
| `partition.prefix`     | Optional prefix for partition keys                            | Optional    | `"partition."` |

- **Default Value**: Specifies the default value assigned if no value is explicitly provided in the configuration.

These properties allow you to customize which fields from the offset and partition of a SourceRecord are added as headers, along with specifying optional prefixes for the header keys. Adjust these configurations based on your specific use case and data requirements.

### Example Configuration

```properties
transforms=InsertSourcePartitionOrOffsetValue
transforms.InsertSourcePartitionOrOffsetValue.type=io.lenses.connect.smt.header.InsertSourcePartitionOrOffsetValue
transforms.InsertSourcePartitionOrOffsetValue.offset.fields=path,line,ts
transforms.InsertSourcePartitionOrOffsetValue.partition.fields=container,prefix
```

### Explanation of Configuration

- `transforms`: This property lists the transformations to be applied to the records.
- `transforms.InsertSourcePartitionOrOffsetValue.type`: Specifies the class implementing the transformation (`InsertSourcePartitionOrOffsetValue` in this case).
- `transforms.InsertSourcePartitionOrOffsetValue.offset.fields`: Defines the fields from the offset to be inserted as headers in the SourceRecord. Replace `path,line,ts` with the actual field names you want to extract from the offset.
- `transforms.InsertSourcePartitionOrOffsetValue.partition.fields`: Defines the fields from the partition to be inserted as headers in the SourceRecord. Replace `container,prefix` with the actual field names you want to extract from the partition.

## Example Usage with Cloud Connectors

### AWS S3, Azure Datalake or GCP Storage

When using this transformation with AWS S3, you can configure your Kafka Connect connector as follows:

```properties
transforms=InsertSourcePartitionOrOffsetValue
transforms.InsertSourcePartitionOrOffsetValue.type=io.lenses.connect.smt.header.InsertSourcePartitionOrOffsetValue
transforms.InsertSourcePartitionOrOffsetValue.offset.fields=path,line,ts
transforms.InsertSourcePartitionOrOffsetValue.partition.fields=container,prefix
```

To customise the header prefix you can also set the header values:

Replace `path,line,ts` and `container,prefix` with the actual field names you are interested in extracting from the partition or offset.

By using `InsertSourcePartitionOrOffsetValue` transformation, you can enrich your data records with additional metadata headers based on partition or offset values before they are delivered to your cloud storage destinations.


### Using the Prefix Feature in InsertSourcePartitionOrOffsetValue Transformation

The prefix feature in `InsertSourcePartitionOrOffsetValue` allows you to prepend a consistent identifier to each header key added based on partition or offset values from SourceRecords.

#### Configuration

Configure the transformation in your Kafka Connect connector properties:

```properties
transforms=InsertSourcePartitionOrOffsetValue
transforms.InsertSourcePartitionOrOffsetValue.type=io.lenses.connect.smt.header.InsertSourcePartitionOrOffsetValue
transforms.InsertSourcePartitionOrOffsetValue.offset.fields=path,line,ts
transforms.InsertSourcePartitionOrOffsetValue.offset.prefix=offset.
transforms.InsertSourcePartitionOrOffsetValue.partition.fields=container,prefix
transforms.InsertSourcePartitionOrOffsetValue.partition.prefix=partition.
```

- `offset.prefix`: Specifies the prefix for headers derived from offset values. Default is `"offset."`.
- `partition.prefix`: Specifies the prefix for headers derived from partition values. Default is `"partition."`.

#### Example Usage

By setting `offset.prefix=offset.` and `partition.prefix=partition.`, headers added based on offset and partition fields will have keys prefixed accordingly in the SourceRecord headers.

This configuration ensures clarity and organization when inserting metadata headers into your Kafka records, distinguishing them based on their source (offset or partition). Adjust prefixes (`offset.prefix` and `partition.prefix`) as per your naming conventions or requirements.