package com.miniredis.types;

import java.io.Serial;
import java.io.Serializable;

public class StringValue implements RedisValue, Serializable {
    @Serial private static final long serialVersionUID = 1L;
    private volatile String value;

    public StringValue(String value) { this.value = value; }
    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    @Override public String toString() { return value; }
}
