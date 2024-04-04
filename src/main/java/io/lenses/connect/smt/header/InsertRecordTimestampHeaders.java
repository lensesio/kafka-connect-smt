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

import java.time.Instant;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * A transformer which takes the system time (wall-clock) and inserts a header year, month, day,
 * hour, minute, second and day. The benefit over the InsertField SMT is that the payload is not
 * modified leading to less memory used and less CPU time.
 *
 * @param <R> the record type
 */
public class InsertRecordTimestampHeaders<R extends ConnectRecord<R>>
    extends InsertTimestampHeaders<R> {

  public InsertRecordTimestampHeaders() {}

  @Override
  protected Instant getInstant(R r) {
    return r.timestamp() == null ? Instant.now() : Instant.ofEpochMilli(r.timestamp());
  }
}
