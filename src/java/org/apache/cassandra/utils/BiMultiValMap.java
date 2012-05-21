package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 *
 * A variant of BiMap which does not enforce uniqueness of values. This means the inverse
 * is a Multimap.
 *
 * @param <K>
 * @param <V>
 */
public class BiMultiValMap<K, V> implements Map<K, V>
{
    protected final Map<K, V> forwardMap;
    protected final Multimap<V, K> reverseMap;

    public BiMultiValMap()
    {
        this.forwardMap = new HashMap<K, V>();
        this.reverseMap = Multimaps.synchronizedMultimap(HashMultimap.<V, K>create());
    }

    protected BiMultiValMap(Map<K, V> forwardMap, Multimap<V, K> reverseMap)
    {
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
    }

    public BiMultiValMap(BiMultiValMap<K, V> map)
    {
        this();
        forwardMap.putAll(map);
        reverseMap.putAll(map.inverse());
    }

    public Multimap<V, K> inverse()
    {
        return Multimaps.unmodifiableMultimap(reverseMap);
    }

    public synchronized void clear()
    {
        forwardMap.clear();
        reverseMap.clear();
    }

    public synchronized boolean containsKey(Object key)
    {
        return forwardMap.containsKey(key);
    }

    public synchronized boolean containsValue(Object value)
    {
        return reverseMap.containsKey(value);
    }

    public synchronized Set<Map.Entry<K, V>> entrySet()
    {
        return forwardMap.entrySet();
    }

    public synchronized V get(Object key)
    {
        return forwardMap.get(key);
    }

    public synchronized boolean isEmpty()
    {
        return forwardMap.isEmpty();
    }

    public synchronized Set<K> keySet()
    {
        return forwardMap.keySet();
    }

    public synchronized V put(K key, V value)
    {
        V oldVal = forwardMap.put(key, value);
        if (oldVal != null)
            reverseMap.remove(oldVal, key);
        reverseMap.put(value, key);
        return oldVal;
    }

    public synchronized void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public synchronized V remove(Object key)
    {
        V oldVal = forwardMap.remove(key);
        reverseMap.remove(oldVal, key);
        return oldVal;
    }

    public synchronized Collection<K> removeValue(V value)
    {
        Collection<K> keys = reverseMap.removeAll(value);
        for (K key : keys)
            forwardMap.remove(key);
        return keys;
    }

    public synchronized int size()
    {
        return forwardMap.size();
    }

    public synchronized Collection<V> values()
    {
        return reverseMap.keys();
    }
}
