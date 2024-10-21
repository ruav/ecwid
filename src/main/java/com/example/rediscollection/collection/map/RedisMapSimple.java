package com.example.rediscollection.collection.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.JedisCommands;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RedisMapSimple extends RedisMap {

    protected JedisCommands jedis;
    protected final String prefix;
    //Here we're using a simple blocking on the app level.
    // Instead we can use Redisson, Jedis-Lock, etc for redis level blocking.
    protected final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public RedisMapSimple() {
        this(new Jedis(), null);
    }

    public RedisMapSimple(JedisCommands jedis, String prefix) {
        this.jedis = jedis;
        this.prefix = prefix == null ? defaultPrefix : prefix;
    }

    @Override
    public int size() {
        rwLock.readLock().lock();
        try {
            return (int) jedis.hlen(prefix);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        rwLock.readLock().lock();
        try {
            return size() == 0;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        rwLock.readLock().lock();
        try {
            return jedis.hexists(prefix, (String) key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Integer get(Object key) {
        rwLock.readLock().lock();
        try {

            String value = jedis.hget(prefix, (String) key);
            return value != null ? Integer.valueOf(value) : null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Integer put(String key, Integer value) {
        rwLock.readLock().lock();
        try {
            jedis.hset(prefix, key, String.valueOf(value));
            return value;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends Integer> m) {
        rwLock.readLock().lock();
        try {
            Map<String, String> stringMap = m.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().toString(), (a, b) -> b));
            jedis.hmset(prefix, stringMap);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Integer remove(Object key) {
        rwLock.readLock().lock();
        try {
            jedis.hdel(prefix, (String) key);
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        rwLock.readLock().lock();
        try {

            if (!(value instanceof Integer)) {
                return false;
            }
            return jedis.hvals(prefix).contains(value.toString());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void clear() {
        rwLock.readLock().lock();
        try {
            jedis.del(prefix);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Collection<Integer> values() {
        rwLock.readLock().lock();
        try {

            Map<String, String> redisMap = jedis.hgetAll(prefix);
            return redisMap.values().stream()
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Set<Entry<String, Integer>> entrySet() {
        rwLock.readLock().lock();
        try {
            Map<String, String> redisMap = jedis.hgetAll(prefix);
            return redisMap.entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), Integer.valueOf(entry.getValue())))
                    .collect(Collectors.toSet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Set<String> keySet() {
        rwLock.readLock().lock();
        try {
            return jedis.hkeys(prefix);
        } finally {
            rwLock.readLock().unlock();
        }
    }

}
