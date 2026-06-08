# Mini-Redis — Java Edition

A fully-functional, in-memory Redis-compatible server written in pure Java 21.
No external dependencies. Supports the real RESP protocol, so any Redis client
(redis-cli, Jedis, Lettuce, etc.) can connect out of the box.

---

## Features

| Feature | Details |
|---|---|
| **Data types** | String, List, Hash |
| **TTL / Expiry** | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST` |
| **Pub/Sub** | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |
| **AOF Persistence** | Every write appended to `appendonly.aof`; replayed on startup |
| **Snapshot** | Binary snapshot (`dump.rdb`) saved every 5 min & on shutdown |
| **TCP Server** | Full RESP protocol — connect with `redis-cli -p 6379` |
| **In-process API** | Use `RedisStore` directly as a library |
| **Thread-safe** | `ConcurrentHashMap` + active TTL reaper thread |

---

## Project Layout

```
mini-redis/
├── pom.xml
└── src/
    ├── main/java/com/miniredis/
    │   ├── MiniRedis.java                  ← main entry point
    │   ├── core/
    │   │   ├── RedisStore.java             ← in-memory store + TTL engine
    │   │   ├── RedisException.java
    │   │   └── WrongTypeException.java
    │   ├── types/
    │   │   ├── RedisValue.java             ← marker interface
    │   │   ├── StringValue.java
    │   │   ├── ListValue.java
    │   │   └── HashValue.java
    │   ├── server/
    │   │   ├── RedisServer.java            ← TCP accept loop
    │   │   ├── ClientSession.java          ← per-connection state
    │   │   ├── CommandHandler.java         ← RESP → store dispatch
    │   │   └── RespProtocol.java           ← RESP encoder/decoder
    │   ├── persistence/
    │   │   ├── AOFWriter.java              ← append-only file
    │   │   └── SnapshotManager.java        ← binary snapshots
    │   └── pubsub/
    │       └── PubSubManager.java          ← channel fan-out
    └── test/java/com/miniredis/
        └── MiniRedisTest.java              ← self-contained test suite
```

---

## Build & Run

### Prerequisites
- Java 21+
- Maven 3.8+

### Build fat JAR
```bash
cd mini-redis
mvn package -q
```
This produces `target/mini-redis.jar`.

### Start the server
```bash
# Default: port 6379, AOF + snapshots enabled
java -jar target/mini-redis.jar

# Custom port, disable persistence
java -jar target/mini-redis.jar --port 6380 --no-aof --no-snapshot

# All options
java -jar target/mini-redis.jar \
  --port 6379 \
  --aof appendonly.aof \
  --snapshot dump.rdb \
  --snapshot-interval 60
```

### Connect with redis-cli
```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET name "Mini-Redis"
OK
127.0.0.1:6379> GET name
"Mini-Redis"
```

### Run tests
```bash
# Compile + run the built-in test suite
mvn package -q
java -cp target/mini-redis.jar com.miniredis.MiniRedisTest
```

---

## Supported Commands

### String
| Command | Syntax |
|---|---|
| `SET` | `SET key value [EX sec] [PX ms] [NX\|XX]` |
| `GET` | `GET key` |
| `GETSET` | `GETSET key value` |
| `MSET` | `MSET k1 v1 k2 v2 ...` |
| `MGET` | `MGET k1 k2 ...` |
| `SETNX` | `SETNX key value` |
| `SETEX` | `SETEX key seconds value` |
| `PSETEX` | `PSETEX key millis value` |
| `INCR` | `INCR key` |
| `INCRBY` | `INCRBY key n` |
| `DECR` | `DECR key` |
| `DECRBY` | `DECRBY key n` |
| `APPEND` | `APPEND key value` |
| `STRLEN` | `STRLEN key` |
| `DEL` | `DEL key [key ...]` |
| `EXISTS` | `EXISTS key` |

### List
| Command | Syntax |
|---|---|
| `LPUSH` | `LPUSH key val [val ...]` |
| `RPUSH` | `RPUSH key val [val ...]` |
| `LPOP` | `LPOP key` |
| `RPOP` | `RPOP key` |
| `LRANGE` | `LRANGE key start stop` |
| `LLEN` | `LLEN key` |

### Hash
| Command | Syntax |
|---|---|
| `HSET` | `HSET key field value` |
| `HGET` | `HGET key field` |
| `HMSET` | `HMSET key f1 v1 f2 v2 ...` |
| `HMGET` | `HMGET key f1 f2 ...` |
| `HDEL` | `HDEL key field` |
| `HGETALL` | `HGETALL key` |
| `HKEYS` | `HKEYS key` |
| `HVALS` | `HVALS key` |
| `HEXISTS` | `HEXISTS key field` |
| `HLEN` | `HLEN key` |

### TTL
| Command | Syntax |
|---|---|
| `EXPIRE` | `EXPIRE key seconds` |
| `PEXPIRE` | `PEXPIRE key millis` |
| `EXPIREAT` | `EXPIREAT key unix-timestamp` |
| `TTL` | `TTL key` |
| `PTTL` | `PTTL key` |
| `PERSIST` | `PERSIST key` |

### Pub/Sub
| Command | Syntax |
|---|---|
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` |
| `PUBLISH` | `PUBLISH channel message` |

### Server
| Command | Notes |
|---|---|
| `PING` | Returns PONG |
| `KEYS pattern` | Glob patterns (`*`, `?`) |
| `TYPE key` | Returns `string`, `list`, `hash`, or `none` |
| `DEL key [...]` | Delete one or more keys |
| `EXISTS key` | 1 / 0 |
| `RENAME key newkey` | Rename a string key |
| `RANDOMKEY` | Returns a random key |
| `DBSIZE` | Number of keys |
| `FLUSHALL` | Delete all keys |
| `INFO` | Server stats |

---

## Using as a Library

```java
import com.miniredis.core.RedisStore;

RedisStore store = new RedisStore();

// Strings
store.set("greeting", "hello");
store.get("greeting");              // Optional.of("hello")

// TTL
store.setEx("session:abc", "user1", 3_600_000); // 1 hour in ms
store.ttlMillis("session:abc");    // remaining ms

// Lists
store.rpush("queue", "task1", "task2");
store.lpop("queue");               // Optional.of("task1")

// Hashes
store.hset("user:1", "name", "Alice");
store.hgetall("user:1");           // Map{"name" → "Alice"}

store.shutdown(); // stops background TTL reaper
```

---

## Architecture Notes

### Thread safety
- `RedisStore` uses `ConcurrentHashMap` for the key-value store and expiry map.
- A single background thread (`ttl-reaper`) sweeps for expired keys every 100 ms.
- Lazy expiry also happens on every read, so there's no observable staleness.

### AOF format
Commands are written as human-readable lines:
```
SET greeting "hello world"
EXPIRE greeting 3600
HSET user:1 name Alice
```
Quoted strings handle values containing spaces. On startup the file is replayed
through the same `CommandHandler` used for live connections.

### Snapshot format
Java object serialization (`ObjectOutputStream`) with an atomic rename-on-write
pattern to prevent corrupt files. Contains the full store + expiry map + timestamp.
Keys expired while the server was offline are pruned on load.

### RESP protocol
The server speaks Redis Serialization Protocol v2. Both array-format requests
(sent by all Redis clients) and inline format (plain text over telnet) are
supported.

---

## Extending

To add a new command:
1. Add a `case "MYCOMMAND"` branch in `CommandHandler.java`.
2. Implement the logic in `RedisStore.java` if it needs storage.
3. Add the AOF `append(...)` call if the command mutates state.
4. Add a test case in `MiniRedisTest.java`.
