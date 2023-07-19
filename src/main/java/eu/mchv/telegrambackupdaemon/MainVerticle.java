package eu.mchv.telegrambackupdaemon;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import java.nio.file.Path;

public class MainVerticle extends AbstractVerticle {

	private KVDatabase db;
	private KVMap<byte[], byte[]> messagesKV;

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(MainVerticle.class, new DeploymentOptions());
	}

	@Override
	public void start() throws Exception {
		this.db = new KVDatabase(Path.of("telegram-backup"));
		this.messagesKV = db.getKVMap("messages");
	}
}
