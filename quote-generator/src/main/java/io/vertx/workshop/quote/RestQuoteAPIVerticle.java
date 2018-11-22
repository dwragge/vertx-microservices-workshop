package io.vertx.workshop.quote;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * This verticle exposes a HTTP endpoint to retrieve the current / last values of the maker data (quotes).
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class RestQuoteAPIVerticle extends AbstractVerticle {

  private Map<String, JsonObject> quotes = new HashMap<>();

  @Override
  public void start() throws Exception {
    vertx.eventBus().<JsonObject>consumer(GeneratorConfigVerticle.ADDRESS)
        .handler(message -> {
          // TODO Populate the `quotes` map with the received quote
          // Quotes are json objects you can retrieve from the message body
          // The map is structured as follows: name -> quote
          // ----
            JsonObject quote = message.body();
            quotes.put(quote.getString("name"), quote);
          // ----
        });


    vertx.createHttpServer()
        .requestHandler(request -> {
          HttpServerResponse response = request.response()
              .putHeader("content-type", "application/json");

          String name = request.getParam("name");
          if (name == null) {
              response.end(Json.encodePrettily(quotes));
          }
          else {
              if (!quotes.containsKey(name)) {
                  response.setStatusCode(404).end();
              }
              else {
                  response.end(Json.encodePrettily(quotes.get(name)));
              }
          }
        })
        .listen(config().getInteger("http.port"), ar -> {
          if (ar.succeeded()) {
            System.out.println("Server started");
          } else {
            System.out.println("Cannot start the server: " + ar.cause());
          }
        });
  }
}
