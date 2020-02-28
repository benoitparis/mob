package paris.benoit.mob.cluster.utils;

import java.util.HashMap;

// https://stackoverflow.com/questions/6389122/does-a-hashmap-with-a-getandwait-method-exist-e-g-a-blockingconcurrenthashma
public class TransferMap<K, V> {

    private HashMap<K, V> backingMap = new HashMap<K, V>();

    private final Object lock = new Object();

    public V getAndWait(Object key) {
        V value;
        synchronized(lock) {
            do {
                value = backingMap.get(key);
                if (value == null) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } while(value == null);
        }
        return value;
    }

    public V put(K key, V value){
        synchronized(lock) {
            backingMap.put(key, value);
            lock.notifyAll();
        }
        return value;
    }

    @Override
    public String toString() {
        return "TransferMap{" +
                "backingMap=" + backingMap +
                '}';
    }
}
