package com.redis;
import com.redis.server.ClientHandler;
import com.redis.storage.DataStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class RedisServer {
    private static final int PORT = 6379;

    public static void main(String[] args) {
        DataStore store = new DataStore();
        // Virtual threads handle high concurrency with near-zero memory footprint
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis Lite Server listening on port " + PORT + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket, store));
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}
