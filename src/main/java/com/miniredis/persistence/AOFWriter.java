package com.miniredis.persistence;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.logging.*;

/**
 * Append-Only File writer. Every mutating command is serialised as RESP
 * and appended to the AOF file. On startup, replay the file to restore state.
 */
public class AOFWriter implements Closeable {

    private static final Logger log = Logger.getLogger(AOFWriter.class.getName());

    private final Path filePath;
    private BufferedWriter writer;
    private final Object lock = new Object();

    public AOFWriter(String path) throws IOException {
        this.filePath = Paths.get(path);
        this.writer   = Files.newBufferedWriter(filePath,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        log.info("AOF writer opened: " + filePath);
    }

    /**
     * Append a command in inline format: CMD arg1 arg2 ...
     * Each arg is space-separated; args with spaces are quoted.
     */
    public void append(String command, String... args) {
        synchronized (lock) {
            try {
                StringBuilder sb = new StringBuilder(command);
                for (String arg : args) {
                    sb.append(' ');
                    if (arg.contains(" ") || arg.isEmpty()) {
                        sb.append('"').append(arg.replace("\"", "\\\"")).append('"');
                    } else {
                        sb.append(arg);
                    }
                }
                writer.write(sb.toString());
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                log.warning("AOF write error: " + e.getMessage());
            }
        }
    }

    /**
     * Replay the AOF file and return a list of command token lists.
     */
    public static List<List<String>> replay(String path) throws IOException {
        Path p = Paths.get(path);
        if (!Files.exists(p)) return Collections.emptyList();

        List<List<String>> commands = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(p)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                commands.add(parseLine(line));
            }
        }
        log.info("AOF replay: loaded " + commands.size() + " commands from " + path);
        return commands;
    }

    /** Tokenise a line respecting double-quoted arguments. */
    private static List<String> parseLine(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        boolean escape  = false;

        for (char c : line.toCharArray()) {
            if (escape) {
                current.append(c);
                escape = false;
            } else if (c == '\\' && inQuote) {
                escape = true;
            } else if (c == '"') {
                inQuote = !inQuote;
            } else if (c == ' ' && !inQuote) {
                if (!current.isEmpty()) { tokens.add(current.toString()); current.setLength(0); }
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) tokens.add(current.toString());
        return tokens;
    }

    /** Rewrite the AOF from a clean snapshot (compaction). */
    public void rewrite(List<String> lines) throws IOException {
        synchronized (lock) {
            writer.close();
            try (BufferedWriter bw = Files.newBufferedWriter(filePath,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                for (String line : lines) { bw.write(line); bw.newLine(); }
            }
            writer = Files.newBufferedWriter(filePath, StandardOpenOption.APPEND);
            log.info("AOF rewrite complete: " + lines.size() + " commands");
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) { if (writer != null) writer.close(); }
    }
}
