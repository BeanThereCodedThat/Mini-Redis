package com.miniredis.server;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Wraps a TCP socket and tracks per-client state (subscriptions, etc.).
 */
public class ClientSession implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private volatile boolean open = true;
    private final Set<String> subscriptions = new CopyOnWriteArraySet<>();
    private final String id;

    public ClientSession(Socket socket) throws IOException {
        this.socket = socket;
        if (socket != null) {
            this.in  = new BufferedInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
            this.id  = socket.getRemoteSocketAddress().toString();
        } else {
            // Test/mock mode — null socket
            this.in  = InputStream.nullInputStream();
            this.out = OutputStream.nullOutputStream();
            this.id  = "mock";
        }
    }

    public String getId() { return id; }
    public InputStream getInputStream() { return in; }
    public boolean isOpen() { return open && (socket == null || !socket.isClosed()); }

    public synchronized void writeRaw(byte[] data) {
        if (!isOpen() || data == null) return;
        try {
            out.write(data);
            out.flush();
        } catch (IOException ignored) {
            open = false;
        }
    }

    /** Write a response and flush. Returns false if the connection was lost. */
    public boolean write(byte[] data) {
        writeRaw(data);
        return isOpen();
    }

    @Override
    public void close() {
        open = false;
        if (socket != null) { try { socket.close(); } catch (IOException ignored) {} }
    }

    // ── Pub/Sub tracking ────────────────────────────────────────────────────────

    public void addSubscription(String channel)    { subscriptions.add(channel); }
    public void removeSubscription(String channel) { subscriptions.remove(channel); }
    public Set<String> getSubscriptions()          { return Collections.unmodifiableSet(subscriptions); }
    public int subscriptionCount()                 { return subscriptions.size(); }
    public boolean isSubscribed()                  { return !subscriptions.isEmpty(); }
}
