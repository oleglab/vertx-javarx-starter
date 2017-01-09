package io.vertx.starter;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.handler.BodyHandler;

import java.net.InetAddress;
import java.time.Instant;

public class MainVerticle extends AbstractVerticle {

  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public static JsonArray eventLog = new JsonArray();
  public static JsonArray payload = new JsonArray();

  @Override
  public void start(io.vertx.core.Future<Void> startFuture) throws Exception {

    // Create a router object.
    Router router = Router.router(vertx);

    EventBus eb = vertx.eventBus();

    eb.consumer("vertx.cluster.echo", message -> {
      Instant receivedTime = Instant.now();
      JsonObject event = new JsonObject(message.body().toString());

      eventLog.add(event);
      logger.info("received a message: " + event.encodePrettily());
      Instant createdTime = event.getInstant("created");
      //JsonArray payload = event.getJsonArray("payload");
      //event.put("payload", payload.size());
      event.put("received", receivedTime);
      event.put("latency", (receivedTime.getNano() - createdTime.getNano())/1000000);
      logger.info("event latency: " + (receivedTime.getNano() - createdTime.getNano())/1000000);

      while (eventLog.size() > 10) {
        eventLog.remove(0);
      }

    });

    eb.consumer("vertx.cluster.payload", message -> {
      Instant receivedTime = Instant.now();
      JsonObject event = new JsonObject(message.body().toString());

      eventLog.add(event);
      payload = event.getJsonArray("payload");
      logger.info("received a payload: " + payload.size()/1000 + "Kb");
      Instant createdTime = event.getInstant("created");
      event.put("received", receivedTime);
      event.put("latency", (receivedTime.getNano() - createdTime.getNano())/1000000);
      logger.info("event latency: " + (receivedTime.getNano() - createdTime.getNano())/1000000);

      while (eventLog.size() > 10) {
        eventLog.remove(0);
      }

    });


    final InetAddress ip = InetAddress.getLocalHost();

    // Bind get "/" to generate event.
    router.get("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();

      JsonObject event = new JsonObject();
      event.put("host", Json.encode(ip.toString()));
      event.put("payload", payload);
      event.put("created", Instant.now());
      eb.publish("vertx.cluster.echo", event);

      JsonObject reply = new JsonObject();
      reply.put("node", Json.encode(ip.toString()));
      reply.put("events", eventLog);
      reply.put("payload", payload);

      response
        .putHeader("content-type", "application/json; charset=utf-8")
        .end(reply.encodePrettily());
    });

    // Bind put "/payload" to set payload.
    router.route().handler(BodyHandler.create());
    router.put("/payload").produces("application/json").consumes("application/json").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();


      JsonObject event = new JsonObject();
      event.put("host", Json.encode(ip.toString()));
      event.put("created", Instant.now());
      JsonArray object = routingContext.getBodyAsJsonArray();
      event.put("payload", object);

      eb.publish("vertx.cluster.payload", event);

      response
        .setStatusCode(204)
        .end();
    });


    // Bind get "/" to get payload.
    router.get("/payload").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();

      JsonObject reply = new JsonObject();
      reply.put("node", Json.encode(ip.toString()));
      reply.put("payload", payload);

      response
        .putHeader("content-type", "application/json; charset=utf-8")
        .setStatusCode(200)
        .end(reply.encode());
    });

    // Create the HTTP server
    HttpServer httpServer = vertx
      .createHttpServer(
        // Retrieve the port from the configuration, default to 8080.
        new HttpServerOptions().setPort(config().getInteger("http.port", 8080))
      );

    // Pass the "accept" method to the request handler.
    httpServer.requestStream().toObservable().subscribe(router::accept);

    // Start listening on the port
    httpServer
      .listenObservable()
      .subscribe(
        server -> {
          // Server is listening

          JsonObject event = new JsonObject();
          event.put("host", Json.encode(ip.toString()));
          event.put("created", Instant.now());
          eb.publish("vertx.cluster.echo", event);

          startFuture.complete();
        },
        failure -> {
          // Server could not start
          startFuture.fail(failure.getCause());
        }
      );
  }
}
