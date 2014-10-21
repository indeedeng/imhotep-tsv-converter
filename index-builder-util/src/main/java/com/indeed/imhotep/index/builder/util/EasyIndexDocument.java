package com.indeed.imhotep.index.builder.util;

import java.util.HashMap;

/**
 * @author mmorrison
 */
public class EasyIndexDocument extends HashMap<String,String> {
    public long timestamp = 0;
    public String uid;

    public void put(String key, long value) {
        put(key, Long.toString(value, 10));
    }
    public void put(String key, int value) {
        put(key, Integer.toString(value, 10));
    }
    public void put(String key, CharSequence value) {
        put(key, value.toString());
    }
}
