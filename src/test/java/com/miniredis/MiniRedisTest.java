package com.miniredis;

import com.miniredis.core.*;
import com.miniredis.persistence.*;
import com.miniredis.pubsub.PubSubManager;
import com.miniredis.server.*;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Self-contained integration tests (no JUnit dependency).
 * Run: java -cp mini-redis.jar com.miniredis.MiniRedisTest
 */
public class MiniRedisTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("═══════════════════════════════════════");
        System.out.println("  Mini-Redis Test Suite");
        System.out.println("═══════════════════════════════════════\n");

        testStrings();
        testTTL();
        testLists();
        testHashes();
        testPubSub();
        testAOF();
        testRespProtocol();
        testTcpServer();

        System.out.println("\n═══════════════════════════════════════");
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        System.out.println("═══════════════════════════════════════");
        System.exit(failed > 0 ? 1 : 0);
    }

    // ─── String tests ─────────────────────────────────────────────────────────

    static void testStrings() {
        RedisStore store = new RedisStore();
        System.out.println("── String commands ──");

        store.set("hello", "world");
        assertEquals("GET basic",        "world", store.get("hello").orElse(null));
        assertEquals("GET missing",      null,    store.get("nokey").orElse(null));

        store.set("counter", "10");
        assertEquals("INCR",             11L,     store.incr("counter", 1));
        assertEquals("INCRBY 5",         16L,     store.incr("counter", 5));
        assertEquals("DECRBY 3",         13L,     store.incr("counter", -3));

        store.set("a", "alpha");
        store.set("b", "beta");
        store.del("a", "b");
        assertEquals("DEL removes a",    null,    store.get("a").orElse(null));
        assertEquals("DEL removes b",    null,    store.get("b").orElse(null));

        store.set("old", "value");
        Optional<String> prev = store.getSet("old", "new");
        assertEquals("GETSET returns old", "value", prev.orElse(null));
        assertEquals("GETSET sets new",    "new",   store.get("old").orElse(null));

        store.shutdown();
    }

    // ─── TTL tests ────────────────────────────────────────────────────────────

    static void testTTL() throws Exception {
        RedisStore store = new RedisStore();
        System.out.println("── TTL / expiry ──");

        store.setEx("temp", "value", 200); // 200ms
        assertEquals("PTTL set key exists",  "value", store.get("temp").orElse(null));
        Thread.sleep(300);
        assertEquals("Key expired after TTL", null,    store.get("temp").orElse(null));

        store.set("persist", "stay");
        store.setExpiry("persist", 500);
        store.persist("persist");
        Thread.sleep(600);
        assertEquals("PERSIST prevents expiry", "stay", store.get("persist").orElse(null));

        store.set("ttlkey", "x");
        assertEquals("TTL on no-expiry key", -1L, store.ttlMillis("ttlkey"));
        assertEquals("TTL on missing key",   -2L, store.ttlMillis("missingkey"));

        store.shutdown();
    }

    // ─── List tests ──────────────────────────────────────────────────────────

    static void testLists() {
        RedisStore store = new RedisStore();
        System.out.println("── List commands ──");

        store.rpush("mylist", "a", "b", "c");
        assertEquals("LLEN",              3L,               store.llen("mylist"));
        assertEquals("LRANGE all",        List.of("a","b","c"), store.lrange("mylist", 0, -1));
        assertEquals("LRANGE [0,1]",      List.of("a","b"),     store.lrange("mylist", 0, 1));

        store.lpush("mylist", "z");
        assertEquals("LPUSH puts at head", "z", store.lpop("mylist").orElse(null));
        assertEquals("RPOP gets tail",     "c", store.rpop("mylist").orElse(null));
        assertEquals("LLEN after pops",   2L, store.llen("mylist"));

        // Empty list should be removed from store
        store.lpop("mylist");
        store.lpop("mylist");
        assertFalse("Empty list removed", store.exists("mylist"));

        store.shutdown();
    }

    // ─── Hash tests ──────────────────────────────────────────────────────────

    static void testHashes() {
        RedisStore store = new RedisStore();
        System.out.println("── Hash commands ──");

        assertEquals("HSET new field",    1L, store.hset("user:1", "name", "Alice"));
        assertEquals("HSET existing",     0L, store.hset("user:1", "name", "Bob"));
        assertEquals("HGET",              "Bob",  store.hget("user:1", "name").orElse(null));
        assertEquals("HGET missing field", null,  store.hget("user:1", "nope").orElse(null));

        store.hset("user:1", "age", "30");
        assertEquals("HLEN",             2L, store.hlen("user:1"));

        Map<String,String> all = store.hgetall("user:1");
        assertEquals("HGETALL name", "Bob", all.get("name"));
        assertEquals("HGETALL age",  "30",  all.get("age"));

        assertTrue("HEXISTS existing",  store.hexists("user:1", "name"));
        assertFalse("HEXISTS missing",  store.hexists("user:1", "email"));

        store.hdel("user:1", "age");
        assertEquals("HLEN after HDEL", 1L, store.hlen("user:1"));

        store.shutdown();
    }

    // ─── Pub/Sub tests ───────────────────────────────────────────────────────

    static void testPubSub() throws Exception {
        System.out.println("── Pub/Sub ──");
        PubSubManager ps = new PubSubManager();

        // Mock session
        List<byte[]> received = new CopyOnWriteArrayList<>();
        var mockSession = new com.miniredis.server.ClientSession(null) {
            @Override public synchronized void writeRaw(byte[] data) { received.add(data); }
            @Override public boolean isOpen() { return true; }
            @Override public void close() {}
        };

        ps.subscribe("news", mockSession);
        int count = ps.publish("news", "hello!");
        assertEquals("PUBLISH count",     1, count);
        assertEquals("Message delivered", 1, received.size());

        ps.unsubscribe("news", mockSession);
        int after = ps.publish("news", "nobody listening");
        assertEquals("After unsub: 0 receivers", 0, after);
    }

    // ─── AOF tests ───────────────────────────────────────────────────────────

    static void testAOF() throws Exception {
        System.out.println("── AOF persistence ──");
        Path tmp = Files.createTempFile("miniredis-test", ".aof");

        try (AOFWriter aof = new AOFWriter(tmp.toString())) {
            aof.append("SET", "foo", "bar");
            aof.append("SET", "greeting", "hello world");
            aof.append("DEL", "foo");
        }

        List<List<String>> cmds = AOFWriter.replay(tmp.toString());
        assertEquals("AOF: 3 commands",        3,       cmds.size());
        assertEquals("AOF: SET key",           "SET",   cmds.get(0).get(0));
        assertEquals("AOF: SET value",         "bar",   cmds.get(0).get(2));
        assertEquals("AOF: quoted value",      "hello world", cmds.get(1).get(2));

        Files.deleteIfExists(tmp);
    }

    // ─── RESP protocol tests ─────────────────────────────────────────────────

    static void testRespProtocol() throws Exception {
        System.out.println("── RESP protocol ──");

        byte[] ok = RespProtocol.ok();
        assertEquals("Simple string starts with +", '+', (char) ok[0]);

        byte[] err = RespProtocol.error("ERR test");
        assertEquals("Error starts with -", '-', (char) err[0]);

        byte[] num = RespProtocol.integer(42);
        assertEquals("Integer starts with :", ':', (char) num[0]);

        byte[] bulk = RespProtocol.bulkString("hello");
        assertEquals("Bulk string starts with $", '$', (char) bulk[0]);

        byte[] arr = RespProtocol.array(List.of("SET", "key", "val"));
        assertEquals("Array starts with *", '*', (char) arr[0]);

        // Decode round-trip
        byte[] encoded = RespProtocol.array(List.of("GET", "mykey"));
        InputStream is = new ByteArrayInputStream(encoded);
        List<String> decoded = RespProtocol.decode(is);
        assertEquals("Decoded command",  "GET",   decoded.get(0));
        assertEquals("Decoded key",      "mykey", decoded.get(1));
    }

    // ─── TCP server integration test ─────────────────────────────────────────

    static void testTcpServer() throws Exception {
        System.out.println("── TCP server (integration) ──");

        RedisStore store = new RedisStore();
        store.setPubSubManager(new PubSubManager());
        RedisServer server = new RedisServer(16379, store);
        server.start();
        Thread.sleep(200);

        try (Socket s = new Socket("127.0.0.1", 16379);
             OutputStream out = s.getOutputStream();
             InputStream  in  = s.getInputStream()) {

            // Send PING
            out.write(RespProtocol.array(List.of("PING")));
            out.flush();
            List<String> pong = RespProtocol.decode(in);
            assertEquals("TCP PING", "PONG", pong == null ? "" : pong.get(0));

            // SET + GET via wire
            out.write(RespProtocol.array(List.of("SET", "tcpkey", "tcpval")));
            out.flush();
            RespProtocol.decode(in); // consume OK

            out.write(RespProtocol.array(List.of("GET", "tcpkey")));
            out.flush();
            List<String> getResult = RespProtocol.decode(in);
            assertEquals("TCP GET", "tcpval", getResult == null ? "" : getResult.get(0));
        }

        server.stop();
        store.shutdown();
    }

    // ─── Assert helpers ───────────────────────────────────────────────────────

    static void assertEquals(String name, Object expected, Object actual) {
        if (Objects.equals(expected, actual)) {
            System.out.println("  ✓ " + name);
            passed++;
        } else {
            System.out.println("  ✗ " + name + " — expected: " + expected + ", got: " + actual);
            failed++;
        }
    }

    static void assertTrue(String name, boolean condition) {
        assertEquals(name, true, condition);
    }

    static void assertFalse(String name, boolean condition) {
        assertEquals(name, false, condition);
    }
}
