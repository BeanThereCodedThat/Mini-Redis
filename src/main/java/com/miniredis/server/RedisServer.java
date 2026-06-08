package com.miniredis.server;

import com.miniredis.core.RedisStore;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * Non-blocking TCP server that listens on a given port and handles each client
 * connection in a dedicated thread from the pool.
 */
public class RedisServer {

    private static final Logger log = Logger.getLogger(RedisServer.class.getName());

    private final int port;
    private final RedisStore store;
    private final CommandHandler handler;
    private final ExecutorService pool;
    private volatile ServerSocket serverSocket;
    private volatile boolean running = false;
    private final Set<ClientSession> clients = ConcurrentHashMap.newKeySet();

    public RedisServer(int port, RedisStore store) {
        this.port    = port;
        this.store   = store;
        this.handler = new CommandHandler(store);
        this.pool    = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "redis-client");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running = true;
        log.info("Mini-Redis listening on port " + port);

        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    client.setTcpNoDelay(true);
                    ClientSession session = new ClientSession(client);
                    clients.add(session);
                    pool.submit(() -> handleClient(session));
                } catch (IOException e) {
                    if (running) log.warning("Accept error: " + e.getMessage());
                }
            }
        }, "redis-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        running = false;
        clients.forEach(ClientSession::close);
        pool.shutdown();
        try { serverSocket.close(); } catch (IOException ignored) {}
        log.info("Mini-Redis stopped.");
    }

    public int getPort() { return port; }

    // ─── Per-client read loop ─────────────────────────────────────────────────

    private void handleClient(ClientSession session) {
        log.fine("Client connected: " + session.getId());
        try {
            while (session.isOpen()) {
                List<String> tokens = RespProtocol.decode(session.getInputStream());
                if (tokens == null) break; // EOF

                byte[] response = handler.handle(tokens, session);
                if (response != null) {
                    if (!session.write(response)) break;
                }
            }
        } catch (IOException e) {
            if (session.isOpen()) log.fine("Client IO error: " + e.getMessage());
        } finally {
            // Unsubscribe on disconnect
            if (store.getPubSubManager() != null) {
                store.getPubSubManager().unsubscribeAll(session);
            }
            session.close();
            clients.remove(session);
            log.fine("Client disconnected: " + session.getId());
        }
    }
}
