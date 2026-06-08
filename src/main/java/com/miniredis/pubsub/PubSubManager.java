package com.miniredis.pubsub;

import com.miniredis.server.ClientSession;
import com.miniredis.server.RespProtocol;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Simple channel-based pub/sub.
 * Subscribers are ClientSession objects. Publishing fans out to all subscribers of a channel.
 */
public class PubSubManager {

    // channel → set of subscribed sessions
    private final ConcurrentHashMap<String, Set<ClientSession>> channels = new ConcurrentHashMap<>();

    public void subscribe(String channel, ClientSession session) {
        channels.computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>()).add(session);
        session.addSubscription(channel);
    }

    public void unsubscribe(String channel, ClientSession session) {
        Set<ClientSession> subs = channels.get(channel);
        if (subs != null) {
            subs.remove(session);
            if (subs.isEmpty()) channels.remove(channel);
        }
        session.removeSubscription(channel);
    }

    public void unsubscribeAll(ClientSession session) {
        for (String channel : session.getSubscriptions()) {
            unsubscribe(channel, session);
        }
    }

    /**
     * Publish a message to a channel. Returns the number of receivers.
     */
    public int publish(String channel, String message) {
        Set<ClientSession> subs = channels.get(channel);
        if (subs == null || subs.isEmpty()) return 0;

        // RESP: *3\r\n$7\r\nmessage\r\n$<ch_len>\r\n<ch>\r\n$<msg_len>\r\n<msg>\r\n
        byte[] wire = RespProtocol.array(List.of("message", channel, message));
        int count = 0;
        for (ClientSession s : subs) {
            s.writeRaw(wire);
            count++;
        }
        return count;
    }

    public Map<String, Integer> channelStats() {
        Map<String, Integer> stats = new LinkedHashMap<>();
        channels.forEach((ch, subs) -> stats.put(ch, subs.size()));
        return stats;
    }
}
