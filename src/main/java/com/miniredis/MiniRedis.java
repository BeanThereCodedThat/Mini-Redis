package com.miniredis;

import com.miniredis.core.RedisStore;
import com.miniredis.persistence.*;
import com.miniredis.pubsub.PubSubManager;
import com.miniredis.server.RedisServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * Mini-Redis launcher.
 *
 * Usage:
 *   java -jar mini-redis.jar [port] [--aof <file>] [--snapshot <file>] [--snapshot-interval <seconds>]
 *
 * Defaults:
 *   port              = 6379
 *   aof               = appendonly.aof
 *   snapshot          = dump.rdb
 *   snapshot-interval = 300  (5 minutes)
 */
public class MiniRedis {

    private static final Logger log = Logger.getLogger(MiniRedis.class.getName());

    public static void main(String[] args) throws Exception {
        configureLogging();

        // ── Parse args ────────────────────────────────────────────────────────
        int port             = 6379;
        String aofFile       = "appendonly.aof";
        String snapshotFile  = "dump.rdb";
        int snapshotInterval = 300; // seconds
        boolean noAof        = false;
        boolean noSnapshot   = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port"              -> port             = Integer.parseInt(args[++i]);
                case "--aof"               -> aofFile          = args[++i];
                case "--snapshot"          -> snapshotFile     = args[++i];
                case "--snapshot-interval" -> snapshotInterval = Integer.parseInt(args[++i]);
                case "--no-aof"            -> noAof            = true;
                case "--no-snapshot"       -> noSnapshot       = true;
                default -> {
                    try { port = Integer.parseInt(args[i]); } catch (NumberFormatException ignored) {}
                }
            }
        }

        // ── Bootstrap ─────────────────────────────────────────────────────────
        RedisStore store = new RedisStore();

        // Pub/Sub
        PubSubManager pubSub = new PubSubManager();
        store.setPubSubManager(pubSub);

        // Snapshot: load existing data first
        SnapshotManager snapshot = null;
        if (!noSnapshot) {
            snapshot = new SnapshotManager(snapshotFile, store);
            try {
                snapshot.load();
            } catch (Exception e) {
                log.warning("Could not load snapshot: " + e.getMessage());
            }
        }

        // AOF: replay then attach writer
        if (!noAof) {
            try {
                List<List<String>> aofCommands = AOFWriter.replay(aofFile);
                if (!aofCommands.isEmpty()) {
                    log.info("Replaying " + aofCommands.size() + " AOF commands...");
                    com.miniredis.server.CommandHandler replayHandler =
                        new com.miniredis.server.CommandHandler(store);
                    for (List<String> cmd : aofCommands) {
                        replayHandler.handle(cmd, null);
                    }
                }
                AOFWriter aof = new AOFWriter(aofFile);
                store.setAofWriter(aof);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try { aof.close(); } catch (IOException ignored) {}
                }));
            } catch (IOException e) {
                log.warning("AOF init failed: " + e.getMessage());
            }
        }

        // Periodic snapshot scheduler
        if (!noSnapshot) {
            SnapshotManager finalSnapshot = snapshot;
            ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "snapshot-scheduler");
                t.setDaemon(true);
                return t;
            });
            sched.scheduleAtFixedRate(() -> {
                try { finalSnapshot.save(); }
                catch (IOException e) { log.warning("Snapshot save failed: " + e.getMessage()); }
            }, snapshotInterval, snapshotInterval, TimeUnit.SECONDS);

            // Save on shutdown
            SnapshotManager shutdownSnap = finalSnapshot;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { shutdownSnap.save(); log.info("Final snapshot saved."); }
                catch (IOException e) { log.warning("Final snapshot failed: " + e.getMessage()); }
            }));
        }

        // ── Start TCP server ─────────────────────────────────────────────────
        RedisServer server = new RedisServer(port, store);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            server.stop();
            store.shutdown();
        }));

        printBanner(port, aofFile, snapshotFile, snapshotInterval, noAof, noSnapshot);

        // Keep main thread alive
        Thread.currentThread().join();
    }

    private static void printBanner(int port, String aof, String snap, int interval,
                                    boolean noAof, boolean noSnap) {
        System.out.println("""
            ╔══════════════════════════════════════════════╗
            ║          Mini-Redis  •  Java Edition         ║
            ╠══════════════════════════════════════════════╣""");
        System.out.printf("║  Listening   → 127.0.0.1:%-4d               ║%n", port);
        System.out.printf("║  AOF         → %-29s║%n", noAof  ? "disabled" : aof);
        System.out.printf("║  Snapshots   → %-29s║%n", noSnap ? "disabled" : snap + " every " + interval + "s");
        System.out.println("""
            ║  Commands    → String, List, Hash, TTL       ║
            ║               Pub/Sub, RESP protocol         ║
            ╚══════════════════════════════════════════════╝
            Ready to accept connections. Press Ctrl+C to stop.
            """);
    }

    private static void configureLogging() {
        System.setProperty("java.util.logging.SimpleFormatter.format",
            "%1$tH:%1$tM:%1$tS.%1$tL [%4$s] %5$s%6$s%n");
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (var h : root.getHandlers()) h.setFormatter(new SimpleFormatter());
    }
}
