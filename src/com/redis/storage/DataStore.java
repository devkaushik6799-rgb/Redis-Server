package com.redis.storage;

import java.util.concurrent.ConcurrentHashMap;
public class DataStore {
    private static final class Entry {
        final String value;
        final Long expiresAtMs; // null means no expiry

        Entry(String value, Long expiresAtMs) {
            this.value = value;
            this.expiresAtMs = expiresAtMs;
        }

        boolean isExpired() {
            return expiresAtMs != null && System.currentTimeMillis() > expiresAtMs;
        }
    }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    public void set(String key, String value, Long ttlMillis) {
        Long expiresAt = (ttlMillis != null) ? System.currentTimeMillis() + ttlMillis : null;
        store.put(key, new Entry(value, expiresAt));
    }

    public String get(String key) {
        Entry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        // Passive expiry check: clean up on access if expired
        if (entry.isExpired()) {
            store.remove(key);
            return null;
        }
        return entry.value;
    }

    public boolean delete(String key) {
        Entry removed = store.remove(key);
        return removed != null && !removed.isExpired();
    }

    public boolean exists(String key) {
        return get(key) != null;
    }

    public synchronized Long incr(String key) throws NumberFormatException {
        String val = get(key);
        long num = 0;
        if (val != null) {
            num = Long.parseLong(val);
        }
        num++;
        // Maintain existing TTL if key already has one
        Entry existing = store.get(key);
        Long ttl = existing != null ? existing.expiresAtMs : null;
        store.put(key, new Entry(String.valueOf(num), ttl));
        return num;
    }

    public synchronized Long decr(String key) throws NumberFormatException {
        String val = get(key);
        long num = 0;
        if (val != null) {
            num = Long.parseLong(val);
        }
        num--;
        Entry existing = store.get(key);
        Long ttl = existing != null ? existing.expiresAtMs : null;
        store.put(key, new Entry(String.valueOf(num), ttl));
        return num;
    }
}
