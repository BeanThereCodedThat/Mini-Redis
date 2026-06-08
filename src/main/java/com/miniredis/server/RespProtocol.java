package com.miniredis.server;

import java.io.*;
import java.util.*;

/**
 * Redis Serialization Protocol (RESP) encoder and decoder.
 *
 * Wire format:
 *   Simple String  → +OK\r\n
 *   Error           → -ERR message\r\n
 *   Integer         → :42\r\n
 *   Bulk String     → $6\r\nfoobar\r\n  |  $-1\r\n (null)
 *   Array           → *3\r\n$3\r\nSET\r\n...
 */
public class RespProtocol {

    private static final String CRLF = "\r\n";

    // ─── Encoding ──────────────────────────────────────────────────────────────

    public static byte[] simpleString(String msg) {
        return ("+" + msg + CRLF).getBytes();
    }

    public static byte[] error(String msg) {
        return ("-" + msg + CRLF).getBytes();
    }

    public static byte[] integer(long n) {
        return (":" + n + CRLF).getBytes();
    }

    public static byte[] bulkString(String s) {
        if (s == null) return "$-1\r\n".getBytes();
        return ("$" + s.length() + CRLF + s + CRLF).getBytes();
    }

    public static byte[] nullBulk() {
        return "$-1\r\n".getBytes();
    }

    public static byte[] array(List<String> items) {
        if (items == null) return "*-1\r\n".getBytes();
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(items.size()).append(CRLF);
        for (String item : items) {
            if (item == null) sb.append("$-1").append(CRLF);
            else sb.append('$').append(item.length()).append(CRLF).append(item).append(CRLF);
        }
        return sb.toString().getBytes();
    }

    public static byte[] ok() { return simpleString("OK"); }
    public static byte[] pong() { return simpleString("PONG"); }

    // ─── Decoding ──────────────────────────────────────────────────────────────

    /**
     * Parse one RESP message from the stream.
     * Returns a List<String> of tokens (command + args), or null on EOF.
     */
    public static List<String> decode(InputStream in) throws IOException {
        int first = in.read();
        if (first == -1) return null;

        char type = (char) first;
        switch (type) {
            case '*' -> {
                // Inline array (normal client commands)
                int count = Integer.parseInt(readLine(in));
                List<String> parts = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    int t = in.read();
                    if ((char) t != '$') throw new IOException("Expected bulk string, got " + (char) t);
                    int len = Integer.parseInt(readLine(in));
                    byte[] buf = in.readNBytes(len);
                    readLine(in); // trailing \r\n
                    parts.add(new String(buf));
                }
                return parts;
            }
            default -> {
                // Inline command (redis-cli without RESP, e.g. PING\r\n)
                String rest = readLine(in);
                String line = ((char) first + rest).trim();
                return Arrays.asList(line.split("\\s+"));
            }
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                in.read(); // consume \n
                break;
            }
            sb.append((char) c);
        }
        return sb.toString();
    }
}
