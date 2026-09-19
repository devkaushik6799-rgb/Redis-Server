

package com.redis.storage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ResProtocol {
    public static List<String> readCommand(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null; // Client disconnected
        }
        if (line.isEmpty()) {
            return new ArrayList<>();
        }

        // Standard Redis client sends commands as an array: *<count>\r\n
        if (line.charAt(0) != '*') {
            // Fallback for raw inline commands typed into nc/telnet
            return List.of(line.trim().split("\\s+"));
        }

        int numElements = Integer.parseInt(line.substring(1));
        List<String> command = new ArrayList<>(numElements);

        for (int i = 0; i < numElements; i++) {
            String lengthLine = reader.readLine();
            if (lengthLine == null || lengthLine.charAt(0) != '$') {
                throw new IOException("Malformed RESP token: " + lengthLine);
            }
            int byteLength = Integer.parseInt(lengthLine.substring(1));

            // Read bulk string content
            char[] buffer = new char[byteLength];
            int read = 0;
            while (read < byteLength) {
                int r = reader.read(buffer, read, byteLength - read);
                if (r == -1) throw new IOException("Unexpected EOF while reading bulk string");
                read += r;
            }
            command.add(new String(buffer));

            // Consume trailing \r\n
            reader.readLine();
        }

        return command;
    }

    public static void writeSimpleString(OutputStream out, String msg) throws IOException {
        out.write(("+" + msg + "\r\n").getBytes(StandardCharsets.UTF_8));
    }

    public static void writeError(OutputStream out, String err) throws IOException {
        out.write(("-ERR " + err + "\r\n").getBytes(StandardCharsets.UTF_8));
    }

    public static void writeInteger(OutputStream out, long value) throws IOException {
        out.write((":" + value + "\r\n").getBytes(StandardCharsets.UTF_8));
    }

    public static void writeBulkString(OutputStream out, String value) throws IOException {
        if (value == null) {
            out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8)); // Null bulk string
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + bytes.length + "\r\n" + value + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
    }
}
