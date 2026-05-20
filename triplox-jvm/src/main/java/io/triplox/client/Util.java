package io.triplox.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Small Java collection helpers for Datomic-style transaction construction.
 */
public final class Util {
    private Util() {}

    @SafeVarargs
    public static <T> List<T> list(T... items) {
        var out = new ArrayList<T>(items.length);
        for (T item : items) {
            out.add(item);
        }
        return out;
    }

    public static Map<String, Object> map(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("map requires an even number of arguments");
        }
        var out = new LinkedHashMap<String, Object>(keyValues.length / 2);
        for (int i = 0; i < keyValues.length; i += 2) {
            if (!(keyValues[i] instanceof String key)) {
                throw new IllegalArgumentException("map keys must be String");
            }
            out.put(key, keyValues[i + 1]);
        }
        return out;
    }
}
