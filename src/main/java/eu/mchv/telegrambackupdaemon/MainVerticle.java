package eu.mchv.telegrambackupdaemon;

import eu.mchv.kv.DatabaseAdapter;
import eu.mchv.kv.KVDatabase;
import eu.mchv.kv.KVMap;
import eu.mchv.telegrambackupdaemon.data.File;
import eu.mchv.telegrambackupdaemon.data.Message;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.warp.filesponge.DiskCache;
import org.warp.filesponge.FileSponge;

public class MainVerticle extends AbstractVerticle {

	private KVDatabase db;
	private KVMap<Long, Message> messagesKV;
	private KVMap<Long, File> filesMetdataKV;
	private FileSponge fileSponge;
	private DiskCache storedFiles;

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(MainVerticle.class, new DeploymentOptions());
	}

	@Override
	public void start(Promise<Void> startPromise) {
		vertx.executeBlocking(event -> {
			try {
				this.db = new KVDatabase(Path.of("telegram-backup"));

				this.messagesKV = db.getKVMap("messages").mapTo(
						key -> key != null ? Json.decodeValue(Buffer.buffer(key), Long.class) : null,
						key -> key != null ? Json.encode(key).getBytes(StandardCharsets.UTF_8) : null,
						value -> value != null ? Json.decodeValue(Buffer.buffer(value), Message.class) : null,
						value -> value != null ? Json.encode(value).getBytes(StandardCharsets.UTF_8) : null
				);

				this.filesMetdataKV = db.getKVMap("files-metadata").mapTo(
						key -> key != null ? Json.decodeValue(Buffer.buffer(key), Long.class) : null,
						key -> key != null ? Json.encode(key).getBytes(StandardCharsets.UTF_8) : null,
						value -> value != null ? Json.decodeValue(Buffer.buffer(value), File.class) : null,
						value -> value != null ? Json.encode(value).getBytes(StandardCharsets.UTF_8) : null
				);

				var fileSpongeContent = DatabaseAdapter.getDictionary(db.getKVMap("filesponge-content"));
				var fileSpongeMetadata = DatabaseAdapter.getDictionary(db.getKVMap("filesponge-metadata"));

				this.fileSponge = new FileSponge();

				this.storedFiles = DiskCache.openCustom(fileSpongeContent, fileSpongeMetadata, url -> true);
				fileSponge.registerCache(storedFiles).block();

				event.complete();
			} catch (Exception ex) {
				event.fail(ex);
			}
		});
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		vertx.executeBlocking(event -> {
			try {
				this.messagesKV.close();
				this.db.close();
				this.storedFiles.close();

				event.complete();
			} catch (Exception ex) {
				event.fail(ex);
			}
		});
	}
}
