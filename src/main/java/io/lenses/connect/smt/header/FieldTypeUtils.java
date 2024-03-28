/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at: http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
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
