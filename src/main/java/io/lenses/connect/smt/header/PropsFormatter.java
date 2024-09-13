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

import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Comparator;
import java.util.Map;

/**
 * This class is responsible for formatting properties from a SimpleConfig object.
 * It converts the properties into a string representation in a json-like format.
 */
public class PropsFormatter {

    private final SimpleConfig simpleConfig;

    /**
     * Constructs a new PropsFormatter with the given SimpleConfig.
     *
     * @param simpleConfig the SimpleConfig object containing the properties to be formatted
     */
    public PropsFormatter(SimpleConfig simpleConfig) {
        this.simpleConfig = simpleConfig;
    }

    /**
     * Formats the properties from the SimpleConfig object into a string.
     * The properties are represented as key-value pairs in the format: "key: "value"".
     * All properties are enclosed in curly braces.
     *
     * @return a string representation of the properties
     */
    public String apply() {
        StringBuilder sb = new StringBuilder("{");
        simpleConfig.originalsStrings().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach((entry) -> sb.append(entry.getKey()).append(": \"").append(entry.getValue()).append("\", "));
        sb.delete(sb.length() - 2, sb.length());
        return sb.append("}").toString();
    }
}