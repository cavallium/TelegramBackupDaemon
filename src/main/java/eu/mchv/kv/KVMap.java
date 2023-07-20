package eu.mchv.kv;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface KVMap<K, V> extends Map<K, V>, Closeable {

	default <K2, V2> KVMap<K2, V2> mapTo(Function<K, K2> keySerializer,
			Function<K2, K> keyDeserializer,
			Function<V, V2> valueSerializer,
			Function<V2, V> valueDeserializer) {
		return new KVMap<>() {
			@Override
			public void close() throws IOException {
				KVMap.this.close();
			}

			@Override
			public int size() {
				return KVMap.this.size();
			}

			@Override
			public boolean isEmpty() {
				return KVMap.this.isEmpty();
			}

			@Override
			public boolean containsKey(Object o) {
				//noinspection unchecked
				return KVMap.this.containsKey(keyDeserializer.apply((K2) o));
			}

			@Override
			public boolean containsValue(Object o) {
				//noinspection unchecked
				return KVMap.this.containsValue(valueDeserializer.apply((V2) o));
			}

			@Override
			public V2 get(Object o) {
				//noinspection unchecked
				return valueSerializer.apply(KVMap.this.get(keyDeserializer.apply((K2) o)));
			}

			@Override
			public V2 put(K2 k2, V2 v2) {
				return valueSerializer.apply(KVMap.this.put(keyDeserializer.apply(k2), valueDeserializer.apply(v2)));
			}

			@Override
			public V2 remove(Object o) {
				//noinspection unchecked
				return valueSerializer.apply(KVMap.this.remove(keyDeserializer.apply((K2) o)));
			}

			@Override
			public void putAll(Map<? extends K2, ? extends V2> map) {
				map.forEach(this::put);
			}

			@Override
			public void clear() {
				KVMap.this.clear();
			}

			@Override
			public Set<K2> keySet() {
				return KVMap.this.keySet().stream().map(keySerializer).collect(Collectors.toSet());
			}

			@Override
			public Collection<V2> values() {
				return KVMap.this.values().stream().map(valueSerializer).collect(Collectors.toSet());
			}

			@Override
			public Set<Entry<K2, V2>> entrySet() {
				return KVMap.this
						.entrySet()
						.stream()
						.map(e -> Map.entry(keySerializer.apply(e.getKey()), valueSerializer.apply(e.getValue())))
						.collect(Collectors.toSet());
			}
		};
	}
}
