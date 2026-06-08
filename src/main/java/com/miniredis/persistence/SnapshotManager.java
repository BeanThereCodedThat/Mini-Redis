package com.miniredis.persistence;

import com.miniredis.core.RedisStore;
import com.miniredis.types.RedisValue;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.logging.*;

/**
 * Periodic snapshot persistence (RDB-like).
 * Serialises the store to a binary file using Java ObjectOutputStream.
 * Scheduled by MiniRedis main class.
 */
public class SnapshotManager {

    private static final Logger log = Logger.getLogger(SnapshotManager.class.getName());

    private final String filePath;
    private final RedisStore store;

    public SnapshotManager(String filePath, RedisStore store) {
        this.filePath = filePath;
        this.store    = store;
    }

    /** Save current state to disk atomically (write to .tmp then rename). */
    public synchronized void save() throws IOException {
        Path tmp    = Paths.get(filePath + ".tmp");
        Path target = Paths.get(filePath);

        SnapshotData data = new SnapshotData(
            store.snapshot(),
            store.expirySnapshot(),
            System.currentTimeMillis()
        );

        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))) {
            oos.writeObject(data);
        }

        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        log.info("Snapshot saved → " + target + " (" + data.storeData.size() + " keys)");
    }

    /** Load a snapshot from disk and restore the store. Returns true if loaded. */
    public boolean load() throws IOException, ClassNotFoundException {
        Path p = Paths.get(filePath);
        if (!Files.exists(p)) { log.info("No snapshot found at " + filePath); return false; }

        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(p)))) {
            SnapshotData data = (SnapshotData) ois.readObject();

            // Prune keys that expired while we were offline
            long now = System.currentTimeMillis();
            data.expiryData.entrySet().removeIf(e -> now >= e.getValue());
            data.storeData.keySet().removeIf(k -> !data.expiryData.containsKey(k)
                    && data.expiryData.containsKey(k)); // keep non-expiry keys

            store.loadSnapshot(data.storeData, data.expiryData);
            log.info("Snapshot loaded from " + p + " (" + data.storeData.size() + " keys, saved at "
                + new java.util.Date(data.savedAt) + ")");
            return true;
        }
    }

    // ─── Data container ────────────────────────────────────────────────────────

    private static class SnapshotData implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        final Map<String, RedisValue> storeData;
        final Map<String, Long>       expiryData;
        final long                    savedAt;

        SnapshotData(Map<String, RedisValue> s, Map<String, Long> e, long ts) {
            this.storeData  = new HashMap<>(s);
            this.expiryData = new HashMap<>(e);
            this.savedAt    = ts;
        }
    }
}
