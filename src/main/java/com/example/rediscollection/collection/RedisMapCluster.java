package com.example.rediscollection.collection;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Set;

public class RedisMapCluster extends RedisMapSimple {

    public RedisMapCluster(Set<HostAndPort> redisNodes, String prefix) {
        super(new JedisCluster(redisNodes), prefix);
    }

    public void updateClusterConfig(Set<HostAndPort> newClusterNodes) {
        rwLock.writeLock().lock();
        try {
            if (jedis != null) {
                ((JedisCluster) jedis).close();
            }
            this.jedis = new JedisCluster(newClusterNodes);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

}
