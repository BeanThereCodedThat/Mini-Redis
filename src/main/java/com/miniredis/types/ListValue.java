package com.miniredis.types;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ListValue implements RedisValue, Serializable {
    @Serial private static final long serialVersionUID = 1L;
    private final ConcurrentLinkedDeque<String> deque = new ConcurrentLinkedDeque<>();

    public void lpush(String value) { deque.addFirst(value); }
    public void rpush(String value) { deque.addLast(value); }

    public Optional<String> lpop() { return Optional.ofNullable(deque.pollFirst()); }
    public Optional<String> rpop() { return Optional.ofNullable(deque.pollLast()); }

    public int size() { return deque.size(); }

    public List<String> lrange(int start, int stop) {
        List<String> all = new ArrayList<>(deque);
        int len = all.size();
        if (start < 0) start = Math.max(0, len + start);
        if (stop < 0)  stop  = len + stop;
        stop = Math.min(stop, len - 1);
        if (start > stop || start >= len) return Collections.emptyList();
        return new ArrayList<>(all.subList(start, stop + 1));
    }

    public List<String> getAll() { return new ArrayList<>(deque); }
}
