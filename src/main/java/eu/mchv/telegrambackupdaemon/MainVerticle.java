package eu.mchv.telegrambackupdaemon;

import eu.mchv.kv.KVDatabase;
import eu.mchv.kv.KVMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Message;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class MainVerticle extends AbstractVerticle {

	private KVDatabase db;
	private KVMap<Long, Message> messagesKV;

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
						value -> value != null ? Json.decodeValue(Buffer.buffer(value), TdApi.Message.class) : null,
						value -> value != null ? Json.encode(value).getBytes(StandardCharsets.UTF_8) : null
				);

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
				event.complete();
			} catch (Exception ex) {
				event.fail(ex);
			}
		});
	}
}
