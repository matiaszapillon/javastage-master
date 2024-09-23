package com.sandbox.javachallenge;


import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Jedis;


@Getter
@Setter
public class RedisBackedCache {
    private String redisHost;
    private int redisPort;
    private Jedis jedis;

    public RedisBackedCache(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.jedis = new Jedis(redisHost, redisPort);
    }

    public String get(String key) {
        try {
            return jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void set(String key, String value) {
        try {
            jedis.set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    public void put(String key, String value) {
        try {
            jedis.set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}