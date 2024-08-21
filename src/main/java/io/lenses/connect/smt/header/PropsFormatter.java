package io.lenses.connect.smt.header;

import org.apache.kafka.connect.transforms.util.SimpleConfig;

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
        simpleConfig.originalsStrings().forEach((k, v) -> sb.append(k).append(": \"").append(v).append("\", "));
        sb.delete(sb.length() - 2, sb.length());
        return sb.append("}").toString();
    }
}