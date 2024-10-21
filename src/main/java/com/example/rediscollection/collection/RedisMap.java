package com.example.rediscollection.collection;

import java.util.Map;

public abstract class RedisMap implements Map<String, Integer> {
    protected String defaultPrefix = "myMap";
}
