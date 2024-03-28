package io.lenses.connect.smt.header;

import java.util.Arrays;
import org.apache.kafka.common.config.ConfigException;

class FieldTypeUtils {
  public static FieldTypeAndFields extractFieldTypeAndFields(String from) {
    FieldType fieldType;
    String[] fields = from.split("\\.");
    if (fields.length > 0) {
      if (fields[0].equalsIgnoreCase(FieldTypeConstants.KEY_FIELD)) {
        fieldType = FieldType.KEY;
        // drop the first element
        fields = Arrays.copyOfRange(fields, 1, fields.length);
      } else if (fields[0].equalsIgnoreCase(FieldTypeConstants.TIMESTAMP_FIELD)) {
        fieldType = FieldType.TIMESTAMP;
        // if fields length is > 1, then it is an error since the timestamp is a primitive
        if (fields.length > 1) {
          throw new ConfigException(
              "When using the record timestamp field, the field path should only be '_timestamp'.");
        }
        fields = new String[0];
      } else {
        fieldType = FieldType.VALUE;
        if (fields[0].equalsIgnoreCase(FieldTypeConstants.VALUE_FIELD)) {
          // drop the first element
          fields = Arrays.copyOfRange(fields, 1, fields.length);
        }
      }
    } else {
      fieldType = FieldType.VALUE;
    }

    return new FieldTypeAndFields(fieldType, fields);
  }

  static class FieldTypeAndFields {
    private final FieldType fieldType;
    private final String[] fields;

    public FieldTypeAndFields(FieldType fieldType, String[] fields) {
      this.fieldType = fieldType;
      this.fields = fields;
    }

    public FieldType getFieldType() {
      return fieldType;
    }

    public String[] getFields() {
      return fields;
    }
  }
}
