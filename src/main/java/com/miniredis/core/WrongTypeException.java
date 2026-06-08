package com.miniredis.core;

public class WrongTypeException extends RedisException {
    public WrongTypeException() {
        super("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
