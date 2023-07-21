package eu.mchv.kv;

import eu.mchv.kv.KVDatabase.KVMapImpl;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.disk.StandardRocksDBColumn;
import java.util.concurrent.locks.StampedLock;
import org.rocksdb.Snapshot;

public class DatabaseAdapter {

	public static LLLocalDictionary getDictionary(KVMap<byte[], byte[]> kv) {
		if (kv instanceof KVMapImpl kvMapImpl) {
			var db = kvMapImpl.getDb();
			var dbName = db.getName();
			var cfh = kvMapImpl.getHandle();
			var columnName = kvMapImpl.getName();
			var column = new StandardRocksDBColumn(db, dbName, cfh, null, new StampedLock());

			return new LLLocalDictionary(column,
					dbName,
					columnName,
					DatabaseAdapter::throwUnsupportedSnapshot,
					UpdateMode.ALLOW_UNSAFE
			);

		} else {
			throw new UnsupportedOperationException("This KVMap type is not supported");
		}
	}

	private static Snapshot throwUnsupportedSnapshot(LLSnapshot llSnapshot) {
		throw new UnsupportedOperationException();
	}
}
