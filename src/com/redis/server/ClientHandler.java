package com.redis.server;
import com.redis.storage.ResProtocol;
import com.redis.storage.DataStore;
import com.redis.storage.DataStore;
import com.redis.storage.ResProtocol;


import java.io.*;
import java.net.Socket;
import java.util.List;
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final DataStore store;

    public ClientHandler(Socket socket, DataStore store) {
        this.socket = socket;
        this.store = store;
    }

    @Override
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                OutputStream out = socket.getOutputStream()
        ) {
            while (!socket.isClosed()) {
                List<String> command = ResProtocol.readCommand(reader);
                if (command == null) break; // Client gracefully terminated connection
                if (command.isEmpty()) continue;

                executeCommand(command, out);
                out.flush();
            }
        } catch (IOException e) {
            // Client abruptly reset or dropped connection
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void executeCommand(List<String> tokens, OutputStream out) throws IOException {
        String cmd = tokens.get(0).toUpperCase();

        switch (cmd) {
            case "PING" -> {
                if (tokens.size() > 1) {
                    ResProtocol.writeBulkString(out, tokens.get(1));
                } else {
                    ResProtocol.writeSimpleString(out, "PONG");
                }
            }
            case "ECHO" -> {
                if (tokens.size() < 2) {
                    ResProtocol.writeError(out, "wrong number of arguments for 'echo' command");
                } else {
                    ResProtocol.writeBulkString(out, tokens.get(1));
                }
            }
            case "SET" -> handleSet(tokens, out);
            case "GET" -> {
                if (tokens.size() < 2) {
                    ResProtocol.writeError(out, "wrong number of arguments for 'get' command");
                } else {
                    String value = store.get(tokens.get(1));
                    ResProtocol.writeBulkString(out, value);
                }
            }
            case "EXISTS" -> {
                if (tokens.size() < 2) {
                    ResProtocol.writeError(out, "wrong number of arguments for 'exists' command");
                } else {
                    long count = tokens.subList(1, tokens.size()).stream()
                            .filter(store::exists)
                            .count();
                    ResProtocol.writeInteger(out, count);
                }
            }
            case "DEL" -> {
                if (tokens.size() < 2) {
                    ResProtocol.writeError(out, "wrong number of arguments for 'del' command");
                } else {
                    long count = tokens.subList(1, tokens.size()).stream()
                            .filter(store::delete)
                            .count();
                    ResProtocol.writeInteger(out, count);
                }
            }
            case "INCR" -> handleIncrDecr(tokens, out, true);
            case "DECR" -> handleIncrDecr(tokens, out, false);
            default -> ResProtocol.writeError(out, "unknown command '" + cmd + "'");
        }
    }

    private void handleSet(List<String> tokens, OutputStream out) throws IOException {
        if (tokens.size() < 3) {
            ResProtocol.writeError(out, "wrong number of arguments for 'set' command");
            return;
        }
        String key = tokens.get(1);
        String val = tokens.get(2);
        Long ttlMillis = null;

        // Parse optional arguments: EX seconds, PX milliseconds
        for (int i = 3; i < tokens.size(); i++) {
            String opt = tokens.get(i).toUpperCase();
            if (opt.equals("EX") && i + 1 < tokens.size()) {
                ttlMillis = Long.parseLong(tokens.get(++i)) * 1000L;
            } else if (opt.equals("PX") && i + 1 < tokens.size()) {
                ttlMillis = Long.parseLong(tokens.get(++i));
            }
        }

        store.set(key, val, ttlMillis);
        ResProtocol.writeSimpleString(out, "OK");
    }

    private void handleIncrDecr(List<String> tokens, OutputStream out, boolean incr) throws IOException {

        if (tokens.size() != 2) {
            ResProtocol.writeError(out, "wrong number of arguments for '" + (incr ? "incr" : "decr") + "' command");
            return;
        }
        try {
            long result = incr ? store.incr(tokens.get(1)) : store.decr(tokens.get(1));
            ResProtocol.writeInteger(out, result);
        } catch (NumberFormatException e) {
            ResProtocol.writeError(out, "value is not an integer or out of range");
        }
    }
}
