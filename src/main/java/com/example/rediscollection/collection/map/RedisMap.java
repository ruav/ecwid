package com.example.rediscollection.collection.map;

import java.util.Map;

public abstract class RedisMap implements Map<String, Integer> {
    protected String defaultPrefix = "myMap";
}
