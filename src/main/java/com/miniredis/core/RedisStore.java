package com.miniredis.core;

import com.miniredis.types.*;
import com.miniredis.persistence.AOFWriter;
import com.miniredis.pubsub.PubSubManager;

import java.util.*;
import java.util.regex.Pattern;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core in-memory store. Thread-safe via ConcurrentHashMap + per-key striped locks.
 */
public class RedisStore {

    public enum ValueType { STRING, LIST, HASH, SET }

    private final ConcurrentHashMap<String, RedisValue> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    private final ScheduledExecutorService ttlReaper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ttl-reaper");
        t.setDaemon(true);
        return t;
    });

    private final AtomicLong commandCount = new AtomicLong();
    private volatile AOFWriter aofWriter;
    private volatile PubSubManager pubSubManager;

    public RedisStore() {
        // Active expiry sweep every 100ms
        ttlReaper.scheduleAtFixedRate(this::sweepExpired, 100, 100, TimeUnit.MILLISECONDS);
    }

    public void setAofWriter(AOFWriter aofWriter) { this.aofWriter = aofWriter; }
    public void setPubSubManager(PubSubManager pubSubManager) { this.pubSubManager = pubSubManager; }
    public PubSubManager getPubSubManager() { return pubSubManager; }
    public long getCommandCount() { return commandCount.get(); }
    public int size() { return store.size(); }

    // ─── Expiry ────────────────────────────────────────────────────────────────

    public void setExpiry(String key, long millis) {
        expiry.put(key, System.currentTimeMillis() + millis);
    }

    public void persist(String key) {
        expiry.remove(key);
    }

    public long ttlMillis(String key) {
        Long exp = expiry.get(key);
        if (exp == null) return -1;
        long remaining = exp - System.currentTimeMillis();
        return remaining <= 0 ? -2 : remaining;
    }

    public boolean isExpired(String key) {
        Long exp = expiry.get(key);
        return exp != null && System.currentTimeMillis() >= exp;
    }

    private void sweepExpired() {
        long now = System.currentTimeMillis();
        expiry.forEach((key, exp) -> {
            if (now >= exp) {
                store.remove(key);
                expiry.remove(key);
            }
        });
    }

    private RedisValue getIfNotExpired(String key) {
        if (isExpired(key)) {
            store.remove(key);
            expiry.remove(key);
            return null;
        }
        return store.get(key);
    }

    // ─── String commands ───────────────────────────────────────────────────────

    public void set(String key, String value) {
        store.put(key, new StringValue(value));
        expiry.remove(key);
        commandCount.incrementAndGet();
        appendAof("SET", key, value);
    }

    public void setEx(String key, String value, long millis) {
        store.put(key, new StringValue(value));
        expiry.put(key, System.currentTimeMillis() + millis);
        commandCount.incrementAndGet();
        appendAof("PSETEX", key, String.valueOf(millis), value);
    }

    public Optional<String> get(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return Optional.empty();
        if (!(v instanceof StringValue)) throw new WrongTypeException();
        return Optional.of(((StringValue) v).getValue());
    }

    public boolean del(String... keys) {
        boolean any = false;
        for (String key : keys) {
            if (store.remove(key) != null) {
                expiry.remove(key);
                any = true;
                appendAof("DEL", key);
            }
        }
        commandCount.incrementAndGet();
        return any;
    }

    public boolean exists(String key) {
        return getIfNotExpired(key) != null;
    }

    public long incr(String key, long delta) {
        store.compute(key, (k, v) -> {
            if (v == null) return new StringValue(String.valueOf(delta));
            if (!(v instanceof StringValue)) throw new WrongTypeException();
            try {
                long current = Long.parseLong(((StringValue) v).getValue());
                return new StringValue(String.valueOf(current + delta));
            } catch (NumberFormatException e) {
                throw new RedisException("ERR value is not an integer");
            }
        });
        commandCount.incrementAndGet();
        String result = ((StringValue) store.get(key)).getValue();
        appendAof("INCRBY", key, String.valueOf(delta));
        return Long.parseLong(result);
    }

    public Optional<String> getSet(String key, String newValue) {
        RedisValue old = store.get(key);
        store.put(key, new StringValue(newValue));
        expiry.remove(key);
        commandCount.incrementAndGet();
        appendAof("GETSET", key, newValue);
        if (old instanceof StringValue) return Optional.of(((StringValue) old).getValue());
        return Optional.empty();
    }

    // ─── List commands ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private ListValue getOrCreateList(String key) {
        return (ListValue) store.compute(key, (k, v) -> {
            if (v == null) return new ListValue();
            if (!(v instanceof ListValue)) throw new WrongTypeException();
            return v;
        });
    }

    public long lpush(String key, String... values) {
        ListValue list = getOrCreateList(key);
        for (String v : values) list.lpush(v);
        commandCount.incrementAndGet();
        appendAof("LPUSH", key, values);
        return list.size();
    }

    public long rpush(String key, String... values) {
        ListValue list = getOrCreateList(key);
        for (String v : values) list.rpush(v);
        commandCount.incrementAndGet();
        appendAof("RPUSH", key, values);
        return list.size();
    }

    public Optional<String> lpop(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return Optional.empty();
        if (!(v instanceof ListValue)) throw new WrongTypeException();
        Optional<String> result = ((ListValue) v).lpop();
        if (((ListValue) v).size() == 0) store.remove(key);
        commandCount.incrementAndGet();
        appendAof("LPOP", key);
        return result;
    }

    public Optional<String> rpop(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return Optional.empty();
        if (!(v instanceof ListValue)) throw new WrongTypeException();
        Optional<String> result = ((ListValue) v).rpop();
        if (((ListValue) v).size() == 0) store.remove(key);
        commandCount.incrementAndGet();
        appendAof("RPOP", key);
        return result;
    }

    public List<String> lrange(String key, int start, int stop) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return Collections.emptyList();
        if (!(v instanceof ListValue)) throw new WrongTypeException();
        return ((ListValue) v).lrange(start, stop);
    }

    public long llen(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return 0;
        if (!(v instanceof ListValue)) throw new WrongTypeException();
        return ((ListValue) v).size();
    }

    // ─── Hash commands ─────────────────────────────────────────────────────────

    private HashValue getOrCreateHash(String key) {
        return (HashValue) store.compute(key, (k, v) -> {
            if (v == null) return new HashValue();
            if (!(v instanceof HashValue)) throw new WrongTypeException();
            return v;
        });
    }

    public long hset(String key, String field, String value) {
        HashValue hash = getOrCreateHash(key);
        boolean isNew = !hash.exists(field);
        hash.set(field, value);
        commandCount.incrementAndGet();
        appendAof("HSET", key, field, value);
        return isNew ? 1 : 0;
    }

    public Optional<String> hget(String key, String field) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return Optional.empty();
        if (!(v instanceof HashValue)) throw new WrongTypeException();
        return ((HashValue) v).get(field);
    }

    public boolean hdel(String key, String field) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return false;
        if (!(v instanceof HashValue)) throw new WrongTypeException();
        boolean removed = ((HashValue) v).delete(field);
        if (((HashValue) v).size() == 0) store.remove(key);
        commandCount.incrementAndGet();
        appendAof("HDEL", key, field);
        return removed;
    }

    public Map<String, String> hgetall(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return Collections.emptyMap();
        if (!(v instanceof HashValue)) throw new WrongTypeException();
        return ((HashValue) v).getAll();
    }

    public boolean hexists(String key, String field) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return false;
        if (!(v instanceof HashValue)) throw new WrongTypeException();
        return ((HashValue) v).exists(field);
    }

    public long hlen(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return 0;
        if (!(v instanceof HashValue)) throw new WrongTypeException();
        return ((HashValue) v).size();
    }

    // ─── Snapshot ──────────────────────────────────────────────────────────────

    public Map<String, RedisValue> snapshot() {
        return Collections.unmodifiableMap(new HashMap<>(store));
    }

    public Map<String, Long> expirySnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(expiry));
    }

    public void loadSnapshot(Map<String, RedisValue> data, Map<String, Long> expiryData) {
        store.clear();
        expiry.clear();
        store.putAll(data);
        expiry.putAll(expiryData);
    }

    public Set<String> keys(String pattern) {
        String regex = globToRegex(pattern);
        Set<String> result = new LinkedHashSet<>();
        store.keySet().forEach(k -> {
            if (!isExpired(k) && k.matches(regex)) result.add(k);
        });
        return result;
    }

    public void flushAll() {
        store.clear();
        expiry.clear();
        appendAof("FLUSHALL");
    }

    public ValueType type(String key) {
        RedisValue v = getIfNotExpired(key);
        if (v == null) return null;
        if (v instanceof StringValue) return ValueType.STRING;
        if (v instanceof ListValue)   return ValueType.LIST;
        if (v instanceof HashValue)   return ValueType.HASH;
        return null;
    }

    // ─── Helpers ───────────────────────────────────────────────────────────────

    private void appendAof(String command, String... args) {
        if (aofWriter != null) aofWriter.append(command, args);
    }

    private void appendAof(String command, String key, String[] extra) {
        if (aofWriter == null) return;
        String[] args = new String[1 + extra.length];
        args[0] = key;
        System.arraycopy(extra, 0, args, 1, extra.length);
        aofWriter.append(command, args);
    }

    private static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*' -> sb.append(".*");
                case '?' -> sb.append(".");
                case '.' -> sb.append("\\.");
                default  -> sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return sb.append("$").toString();
    }

    public void shutdown() {
        ttlReaper.shutdown();
    }
}
