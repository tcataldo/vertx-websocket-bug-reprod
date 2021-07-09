package io.vertx.starter;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;

public class WsTest {

	@Test
	public void testWs() throws InterruptedException, ExecutionException, TimeoutException {

		int count = 1_000_000;

		Vertx vxSrv = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
		CompletableFuture<Void> srvListen = new CompletableFuture<>();
		vxSrv.deployVerticle(() -> new AbstractVerticle() {
			public void start() throws Exception {
				HttpServer srv = vxSrv.createHttpServer(new HttpServerOptions());
				srv.webSocketHandler(ws -> {
					System.err.println("ws " + ws);
					MessageProducer<String> sender = vxSrv.eventBus().sender(ws.textHandlerID());
					Thread t = new Thread(() -> {
						for (int i = 0; i < count; i++) {
							if (i % 1000 == 0) {
								System.err.println("W: " + i);
							}
							JsonObject js = new JsonObject().put("log", Integer.toString(i))
									.put("ws-rid", ws.textHandlerID()).put("date", new Date().toString());
							sender.write(js.encode());
							if (sender.writeQueueFull()) {
								System.err.println("QF");
								CompletableFuture<Void> writeBlock = new CompletableFuture<>();
								sender.drainHandler(v -> {
									writeBlock.complete(null);
								});
								writeBlock.join();
							}

						}
					});
					t.start();
				});
				srv.listen(44444, ar -> {
					srvListen.complete(null);
				});
			}
		}, new DeploymentOptions().setInstances(4));

		System.err.println("Waiting for deploy...");
		srvListen.get(10, TimeUnit.SECONDS);

		Vertx vxCli = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
		CompletableFuture<WebSocket> cliConn = new CompletableFuture<>();
		CountDownLatch cdl = new CountDownLatch(count);
		vxCli.createHttpClient(new HttpClientOptions().setDefaultPort(44444).setDefaultHost("127.0.0.1")).webSocket("/",
				wsRes -> {
					if (wsRes.succeeded()) {
						WebSocket ws = wsRes.result();
						cliConn.complete(ws);
						ws.textMessageHandler(str -> {
							ws.pause();
							System.err.println("S: " + str);
							cdl.countDown();
							vxCli.setTimer(100, tid -> ws.resume());
						});
					} else {
						cliConn.completeExceptionally(wsRes.cause());
					}
				});
		cliConn.join();
		System.err.println("con ok, messages....");
		cdl.await();
	}
}
