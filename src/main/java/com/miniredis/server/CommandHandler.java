package com.miniredis.server;

import com.miniredis.core.*;
import com.miniredis.types.*;
import com.miniredis.pubsub.*;

import java.util.*;

/**
 * Translates parsed RESP tokens into RedisStore operations and returns wire-ready bytes.
 */
public class CommandHandler {

    private final RedisStore store;

    public CommandHandler(RedisStore store) {
        this.store = store;
    }

    public byte[] handle(List<String> tokens, ClientSession session) {
        if (tokens == null || tokens.isEmpty()) return RespProtocol.error("ERR empty command");

        String cmd = tokens.get(0).toUpperCase();

        try {
            return switch (cmd) {
                // ── Connection ──────────────────────────────────────────────
                case "PING"    -> RespProtocol.pong();
                case "QUIT"    -> { session.close(); yield RespProtocol.ok(); }
                case "SELECT"  -> RespProtocol.ok(); // single DB stub
                case "COMMAND" -> RespProtocol.simpleString("mini-redis");
                case "INFO"    -> handleInfo();
                case "DBSIZE"  -> RespProtocol.integer(store.size());
                case "FLUSHALL"-> { store.flushAll(); yield RespProtocol.ok(); }
                case "FLUSHDB" -> { store.flushAll(); yield RespProtocol.ok(); }

                // ── String ───────────────────────────────────────────────────
                case "SET"     -> handleSet(tokens);
                case "GET"     -> handleGet(tokens);
                case "DEL"     -> handleDel(tokens);
                case "EXISTS"  -> RespProtocol.integer(store.exists(arg(tokens,1)) ? 1 : 0);
                case "INCR"    -> RespProtocol.integer(store.incr(arg(tokens,1), 1));
                case "INCRBY"  -> RespProtocol.integer(store.incr(arg(tokens,1), longArg(tokens,2)));
                case "DECR"    -> RespProtocol.integer(store.incr(arg(tokens,1), -1));
                case "DECRBY"  -> RespProtocol.integer(store.incr(arg(tokens,1), -longArg(tokens,2)));
                case "GETSET"  -> store.getSet(arg(tokens,1), arg(tokens,2))
                                        .map(RespProtocol::bulkString)
                                        .orElse(RespProtocol.nullBulk());
                case "SETNX"   -> {
                    String k = arg(tokens,1);
                    if (!store.exists(k)) { store.set(k, arg(tokens,2)); yield RespProtocol.integer(1); }
                    yield RespProtocol.integer(0);
                }
                case "SETEX"   -> {
                    store.setEx(arg(tokens,1), arg(tokens,3), longArg(tokens,2)*1000);
                    yield RespProtocol.ok();
                }
                case "PSETEX"  -> {
                    store.setEx(arg(tokens,1), arg(tokens,3), longArg(tokens,2));
                    yield RespProtocol.ok();
                }
                case "MSET"    -> handleMset(tokens);
                case "MGET"    -> handleMget(tokens);
                case "APPEND"  -> {
                    String cur = store.get(arg(tokens,1)).orElse("");
                    String newVal = cur + arg(tokens,2);
                    store.set(arg(tokens,1), newVal);
                    yield RespProtocol.integer(newVal.length());
                }
                case "STRLEN"  -> RespProtocol.integer(store.get(arg(tokens,1)).orElse("").length());

                // ── TTL ──────────────────────────────────────────────────────
                case "EXPIRE"  -> { store.setExpiry(arg(tokens,1), longArg(tokens,2)*1000); yield RespProtocol.integer(1); }
                case "PEXPIRE" -> { store.setExpiry(arg(tokens,1), longArg(tokens,2));      yield RespProtocol.integer(1); }
                case "TTL"     -> {
                    long ms = store.ttlMillis(arg(tokens,1));
                    yield RespProtocol.integer(ms < 0 ? ms : ms / 1000);
                }
                case "PTTL"    -> RespProtocol.integer(store.ttlMillis(arg(tokens,1)));
                case "PERSIST" -> { store.persist(arg(tokens,1)); yield RespProtocol.integer(1); }
                case "EXPIREAT"-> {
                    long epochSec = longArg(tokens,2);
                    long millis = epochSec*1000 - System.currentTimeMillis();
                    store.setExpiry(arg(tokens,1), millis);
                    yield RespProtocol.integer(1);
                }

                // ── List ─────────────────────────────────────────────────────
                case "LPUSH"   -> RespProtocol.integer(store.lpush(arg(tokens,1), tailArgs(tokens,2)));
                case "RPUSH"   -> RespProtocol.integer(store.rpush(arg(tokens,1), tailArgs(tokens,2)));
                case "LPOP"    -> store.lpop(arg(tokens,1)).map(RespProtocol::bulkString).orElse(RespProtocol.nullBulk());
                case "RPOP"    -> store.rpop(arg(tokens,1)).map(RespProtocol::bulkString).orElse(RespProtocol.nullBulk());
                case "LRANGE"  -> RespProtocol.array(store.lrange(arg(tokens,1), intArg(tokens,2), intArg(tokens,3)));
                case "LLEN"    -> RespProtocol.integer(store.llen(arg(tokens,1)));

                // ── Hash ─────────────────────────────────────────────────────
                case "HSET"    -> RespProtocol.integer(store.hset(arg(tokens,1), arg(tokens,2), arg(tokens,3)));
                case "HGET"    -> store.hget(arg(tokens,1), arg(tokens,2)).map(RespProtocol::bulkString).orElse(RespProtocol.nullBulk());
                case "HDEL"    -> RespProtocol.integer(store.hdel(arg(tokens,1), arg(tokens,2)) ? 1 : 0);
                case "HGETALL" -> {
                    Map<String,String> all = store.hgetall(arg(tokens,1));
                    List<String> flat = new ArrayList<>();
                    all.forEach((k,v) -> { flat.add(k); flat.add(v); });
                    yield RespProtocol.array(flat);
                }
                case "HEXISTS" -> RespProtocol.integer(store.hexists(arg(tokens,1), arg(tokens,2)) ? 1 : 0);
                case "HLEN"    -> RespProtocol.integer(store.hlen(arg(tokens,1)));
                case "HKEYS"   -> RespProtocol.array(new ArrayList<>(store.hgetall(arg(tokens,1)).keySet()));
                case "HVALS"   -> RespProtocol.array(new ArrayList<>(store.hgetall(arg(tokens,1)).values()));
                case "HMSET"   -> handleHmset(tokens);
                case "HMGET"   -> handleHmget(tokens);

                // ── Key-level ────────────────────────────────────────────────
                case "KEYS"    -> RespProtocol.array(new ArrayList<>(store.keys(tokens.size() > 1 ? tokens.get(1) : "*")));
                case "TYPE"    -> {
                    RedisStore.ValueType t = store.type(arg(tokens,1));
                    yield RespProtocol.simpleString(t == null ? "none" : t.name().toLowerCase());
                }
                case "RENAME"  -> {
                    String val = store.get(arg(tokens,1)).orElseThrow(() -> new RedisException("ERR no such key"));
                    store.set(arg(tokens,2), val);
                    store.del(arg(tokens,1));
                    yield RespProtocol.ok();
                }
                case "RANDOMKEY" -> {
                    Set<String> keys = store.keys("*");
                    if (keys.isEmpty()) yield RespProtocol.nullBulk();
                    yield RespProtocol.bulkString(keys.iterator().next());
                }

                // ── Pub/Sub ──────────────────────────────────────────────────
                case "SUBSCRIBE"   -> handleSubscribe(tokens, session);
                case "UNSUBSCRIBE" -> handleUnsubscribe(tokens, session);
                case "PUBLISH"     -> handlePublish(tokens);

                default -> RespProtocol.error("ERR unknown command '" + cmd + "'");
            };
        } catch (WrongTypeException e) {
            return RespProtocol.error(e.getMessage());
        } catch (RedisException e) {
            return RespProtocol.error(e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            return RespProtocol.error("ERR wrong number of arguments for '" + cmd + "' command");
        } catch (NumberFormatException e) {
            return RespProtocol.error("ERR value is not an integer or out of range");
        }
    }

    // ─── SET with options (EX, PX, NX, XX) ────────────────────────────────────

    private byte[] handleSet(List<String> tokens) {
        String key   = arg(tokens, 1);
        String value = arg(tokens, 2);
        long exMillis = -1;
        boolean nx = false, xx = false;

        for (int i = 3; i < tokens.size(); i++) {
            switch (tokens.get(i).toUpperCase()) {
                case "EX"  -> { exMillis = Long.parseLong(tokens.get(++i)) * 1000; }
                case "PX"  -> { exMillis = Long.parseLong(tokens.get(++i)); }
                case "NX"  -> nx = true;
                case "XX"  -> xx = true;
            }
        }

        if (nx && store.exists(key)) return RespProtocol.nullBulk();
        if (xx && !store.exists(key)) return RespProtocol.nullBulk();

        if (exMillis > 0) store.setEx(key, value, exMillis);
        else store.set(key, value);
        return RespProtocol.ok();
    }

    private byte[] handleGet(List<String> tokens) {
        return store.get(arg(tokens, 1))
                    .map(RespProtocol::bulkString)
                    .orElse(RespProtocol.nullBulk());
    }

    private byte[] handleDel(List<String> tokens) {
        int deleted = 0;
        for (int i = 1; i < tokens.size(); i++) {
            if (store.del(tokens.get(i))) deleted++;
        }
        return RespProtocol.integer(deleted);
    }

    private byte[] handleMset(List<String> tokens) {
        for (int i = 1; i + 1 < tokens.size(); i += 2) {
            store.set(tokens.get(i), tokens.get(i + 1));
        }
        return RespProtocol.ok();
    }

    private byte[] handleMget(List<String> tokens) {
        List<String> results = new ArrayList<>();
        for (int i = 1; i < tokens.size(); i++) {
            results.add(store.get(tokens.get(i)).orElse(null));
        }
        return RespProtocol.array(results);
    }

    private byte[] handleHmset(List<String> tokens) {
        String key = arg(tokens, 1);
        for (int i = 2; i + 1 < tokens.size(); i += 2) {
            store.hset(key, tokens.get(i), tokens.get(i + 1));
        }
        return RespProtocol.ok();
    }

    private byte[] handleHmget(List<String> tokens) {
        String key = arg(tokens, 1);
        List<String> results = new ArrayList<>();
        for (int i = 2; i < tokens.size(); i++) {
            results.add(store.hget(key, tokens.get(i)).orElse(null));
        }
        return RespProtocol.array(results);
    }

    private byte[] handleInfo() {
        String info = "# Server\r\nredis_version:7.0.0-mini\r\n" +
                      "# Stats\r\ntotal_commands_processed:" + store.getCommandCount() + "\r\n" +
                      "# Keyspace\r\ndb0:keys=" + store.size() + "\r\n";
        return RespProtocol.bulkString(info);
    }

    // ─── Pub/Sub ────────────────────────────────────────────────────────────────

    private byte[] handleSubscribe(List<String> tokens, ClientSession session) {
        if (store.getPubSubManager() == null) return RespProtocol.error("ERR pubsub not enabled");
        PubSubManager ps = store.getPubSubManager();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < tokens.size(); i++) {
            String channel = tokens.get(i);
            ps.subscribe(channel, session);
            // RESP subscribe response: *3\r\n$9\r\nsubscribe\r\n$<ch_len>\r\n<ch>\r\n:<count>\r\n
            List<String> msg = List.of("subscribe", channel, String.valueOf(session.subscriptionCount()));
            session.writeRaw(RespProtocol.array(msg));
        }
        return null; // already wrote directly
    }

    private byte[] handleUnsubscribe(List<String> tokens, ClientSession session) {
        if (store.getPubSubManager() == null) return RespProtocol.error("ERR pubsub not enabled");
        PubSubManager ps = store.getPubSubManager();
        List<String> channels = tokens.subList(1, tokens.size());
        if (channels.isEmpty()) channels = new ArrayList<>(session.getSubscriptions());
        for (String channel : channels) {
            ps.unsubscribe(channel, session);
            List<String> msg = List.of("unsubscribe", channel, String.valueOf(session.subscriptionCount()));
            session.writeRaw(RespProtocol.array(msg));
        }
        return null;
    }

    private byte[] handlePublish(List<String> tokens) {
        if (store.getPubSubManager() == null) return RespProtocol.integer(0);
        String channel = arg(tokens, 1);
        String message = arg(tokens, 2);
        int count = store.getPubSubManager().publish(channel, message);
        return RespProtocol.integer(count);
    }

    // ─── Helpers ───────────────────────────────────────────────────────────────

    private String arg(List<String> tokens, int idx) { return tokens.get(idx); }
    private long longArg(List<String> tokens, int idx) { return Long.parseLong(tokens.get(idx)); }
    private int  intArg(List<String> tokens, int idx)  { return Integer.parseInt(tokens.get(idx)); }
    private String[] tailArgs(List<String> tokens, int from) {
        return tokens.subList(from, tokens.size()).toArray(new String[0]);
    }
}
