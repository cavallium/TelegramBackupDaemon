package eu.mchv.kv;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;

public class KVDatabase extends AbstractImmutableNativeReference {

	private final LRUCache blockCache;
	private final RocksDB db;
	private final Map<String, ColumnFamilyDescriptor> descriptorMap = new HashMap<>();
	private final Map<String, ColumnFamilyHandle> handleMap = new HashMap<>();

	public KVDatabase(Path path) throws RocksDBException {
		super(true);
		Options options = new Options();
		BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
		tableOptions.setBlockSize(16 * 1024);
		tableOptions.setCacheIndexAndFilterBlocks(true);
		tableOptions.setPinL0FilterAndIndexBlocksInCache(true);
		this.blockCache = new LRUCache(64 * SizeUnit.MB);
		tableOptions.setBlockCache(blockCache);
		options.setCreateIfMissing(true)
				.setWriteBufferSize(512 * SizeUnit.MB)
				.setMaxWriteBufferNumber(2)
				.setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
				// table options
				.setTableFormatConfig(tableOptions)
				.setLevelCompactionDynamicLevelBytes(true)
				.setMaxBackgroundJobs(6)
				.setBytesPerSync(1048576)
				.setCompactionPriority(CompactionPriority.MinOverlappingRatio)
				.setCreateMissingColumnFamilies(true)
				.setInfoLogLevel(InfoLogLevel.WARN_LEVEL)
				.setKeepLogFileNum(3)
				.setMaxLogFileSize(10 * SizeUnit.MB);

		var cfs = RocksDB
				.listColumnFamilies(options, path.toString())
				.stream()
				.map(columnFamilyName -> new ColumnFamilyDescriptor(columnFamilyName, new ColumnFamilyOptions(options)))
				.collect(Collectors.toCollection(ArrayList::new));

		if (cfs.isEmpty()) {
			cfs.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions(options)));
		}

		var dbOptions = new DBOptions(options);

		var handles = new ArrayList<ColumnFamilyHandle>(cfs.size());

		this.db = RocksDB.open(dbOptions, path.toString(), cfs, handles);

		for (int i = 0; i < cfs.size(); i++) {
			var cfd = cfs.get(i);
			var name = new String(cfd.getName(), StandardCharsets.UTF_8);
			this.descriptorMap.put(name, cfd);
			this.handleMap.put(name, handles.get(i));
		}
	}

	public KVMap<byte[], byte[]> getKVMap(String name) {
		ColumnFamilyHandle handle;

		synchronized (this) {
			handle = handleMap.get(name);
			if (handle == null) {
				var descriptor = descriptorMap.get(name);
				if (descriptor == null) {
					descriptor = new ColumnFamilyDescriptor(
							name.isEmpty() ? RocksDB.DEFAULT_COLUMN_FAMILY : name.getBytes(StandardCharsets.UTF_8));
					descriptorMap.put(name, descriptor);
				}
				try {
					handle = db.createColumnFamily(descriptor);
					handleMap.put(name, handle);
				} catch (RocksDBException e) {
					throw new RuntimeException(e);
				}
			}
		}

		return new KVMapImpl(name, handle);
	}

	private void checkClosed() {
		if (!isOwningHandle()) {
			throw new IllegalStateException("Closed");
		}
	}

	class KVMapImpl extends AbstractMap<byte[], byte[]> implements KVMap<byte[], byte[]>, Closeable {

		private final AbstractImmutableNativeReference reference = new AbstractImmutableNativeReference(true) {
			@Override
			protected void disposeInternal() {

			}
		};

		private final String name;
		private final ColumnFamilyHandle handle;

		public KVMapImpl(String name, ColumnFamilyHandle handle) {
			this.name = name;
			this.handle = handle;
		}

		private void checkClosed() {
			if (!isOwningHandle()) {
				throw new IllegalStateException("Closed");
			}
			KVDatabase.this.checkClosed();
		}

		@Override
		public void close() {
			reference.close();
		}

		@Override
		public int size() {
			checkClosed();
			int i = 0;
			try (var it = db.newIterator(handle)) {
				it.seekToFirst();
				while (it.isValid() && isOwningHandle()) {
					i++;
					it.next();
					try {
						it.status();
					} catch (RocksDBException e) {
						throw new RuntimeException(e);
					}
				}
			}
			return i;
		}

		@Override
		public boolean isEmpty() {
			checkClosed();
			try (var it = db.newIterator(handle)) {
				it.seekToFirst();
				if (it.isValid() && isOwningHandle()) {
					return false;
				}
			}
			return true;
		}

		@Override
		public byte[] get(Object o) {
			try {
				return db.get(handle, (byte[]) o);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public byte[] put(byte[] bytes, byte[] bytes2) {
			try {
				byte[] prev = this.get(bytes);
				db.put(handle, bytes, bytes2);
				return prev;
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public byte[] remove(Object o) {
			try {
				byte[] prev = this.get(o);
				db.delete(handle, (byte[]) o);
				return prev;
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Set<Entry<byte[], byte[]>> entrySet() {
			checkClosed();
			HashSet<Entry<byte[], byte[]>> entries = new HashSet<>();
			try (var it = db.newIterator(handle)) {
				it.seekToFirst();
				while (it.isValid() && isOwningHandle()) {
					entries.add(Map.entry(it.key(), it.value()));
					it.next();
					try {
						it.status();
					} catch (RocksDBException e) {
						throw new RuntimeException(e);
					}
				}
			}
			return entries;
		}

		public ColumnFamilyHandle getHandle() {
			return handle;
		}

		public String getName() {
			return name;
		}

		public RocksDB getDb() {
			return db;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", KVMapImpl.class.getSimpleName() + "[", "]").add("name='" + name + "'").toString();
		}
	}

	@Override
	public void close() {
		try {
			db.closeE();
			blockCache.close();
		} catch (RocksDBException e) {
			throw new RuntimeException("Can't close the database", e);
		}
	}

	@Override
	protected void disposeInternal() {

	}

	@Override
	public String toString() {
		return "KVDatabase";
	}
}
