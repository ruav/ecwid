package com.example.rediscollection;

import com.example.rediscollection.collection.RedisMap;
import com.example.rediscollection.collection.RedisMapCluster;
import com.example.rediscollection.collection.RedisMapSimple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;

public class RedisMapApp {

    public static void main(String[] args) {

        HostAndPort defaultAddress = new HostAndPort("127.0.0.1", 6379);
        System.out.println("===Testing standalone redis===");
        try {
            RedisMap simple = new RedisMapSimple(new Jedis(defaultAddress), null);
            testMap(simple);
        } catch (Exception e) {
            System.err.println(e);
        }

        System.out.println("===Testing cluster redis===");
        try {
            RedisMap cluster = new RedisMapCluster(Set.of(defaultAddress), "myMap");
            testRedisCluster(cluster);
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public static void testRedisCluster(RedisMap redisMap) {
        testMap(redisMap);
        System.out.println("Change cluster");
        ((RedisMapCluster) redisMap).updateClusterConfig(
                Set.of(new HostAndPort("localhost", 7000),
                        new HostAndPort("localhost", 7001)));
        System.out.println(redisMap.size());
        System.out.println("key1: " + redisMap.get("key1"));
        System.out.println("key2: " + redisMap.get("key2"));
    }

    public static void testMap(Map<String, Integer> redisMap) {

        System.out.println(redisMap.size());
        redisMap.put("key1", 100);
        redisMap.put("key2", 200);

        System.out.println("key1: " + redisMap.get("key1"));
        System.out.println("key2: " + redisMap.get("key2"));
        System.out.println(redisMap.size());

        redisMap.remove("key1");
        System.out.println("After delete key1: " + redisMap.get("key1"));
        redisMap.clear();
        System.out.println(redisMap.size());
    }

}
