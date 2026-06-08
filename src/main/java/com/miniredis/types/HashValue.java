package com.miniredis.types;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HashValue implements RedisValue, Serializable {
    @Serial private static final long serialVersionUID = 1L;
    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    public void set(String field, String value) { map.put(field, value); }
    public Optional<String> get(String field) { return Optional.ofNullable(map.get(field)); }
    public boolean delete(String field) { return map.remove(field) != null; }
    public boolean exists(String field) { return map.containsKey(field); }
    public int size() { return map.size(); }
    public Map<String, String> getAll() { return Collections.unmodifiableMap(new HashMap<>(map)); }
    public Set<String> keys() { return map.keySet(); }
    public Collection<String> values() { return map.values(); }
}
