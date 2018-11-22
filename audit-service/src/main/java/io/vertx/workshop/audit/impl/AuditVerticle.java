package io.vertx.workshop.audit.impl;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient;
import io.vertx.rxjava.ext.asyncsql.PostgreSQLClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.servicediscovery.types.MessageSource;
import io.vertx.workshop.common.RxMicroServiceVerticle;
import rx.Single;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A verticle storing operations in a database (hsql) and providing access to the operations.
 */
public class AuditVerticle extends RxMicroServiceVerticle {

  private static final String DROP_STATEMENT = "DROP TABLE IF EXISTS AUDIT";
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS AUDIT (id INTEGER GENERATED ALWAYS AS IDENTITY, operation json NOT NULL)";
  private static final String INSERT_STATEMENT = "INSERT INTO AUDIT (operation) VALUES (?)";
  private static final String SELECT_STATEMENT = "SELECT * FROM AUDIT ORDER BY ID DESC LIMIT 10";

  private AsyncSQLClient sqlClient;

  /**
   * Starts the verticle asynchronously. The the initialization is completed, it calls
   * `complete()` on the given {@link Future} object. If something wrong happens,
   * `fail` is called.
   *
   * @param future the future to indicate the completion
   */
  @Override
  public void start(Future<Void> future) {
    super.start();

    // creates the sqlClient client.
    sqlClient = PostgreSQLClient.createShared(vertx, config());

    // TODO
    // ----
    Single<Void> databaseReady = initializeDatabase(config().getBoolean("drop", false));
    Single<Void> httpEndpointReady = configureTheHTTPServer()
            .flatMap(server -> {
                System.out.println("Establishing REST Client at localhost:" + server.actualPort());
                return rxPublishHttpEndpoint("audit", "localhost", server.actualPort());
            });
    Single<MessageConsumer<JsonObject>> messageConsumerReady = retrieveThePortfolioMessageSource();
    Single<MessageConsumer<JsonObject>> readySingle = Single.zip(
            databaseReady,
            httpEndpointReady,
            messageConsumerReady,
            (db, http, consumer) -> consumer
    );
    // ----

    // signal a verticle start failure
    readySingle.doOnSuccess(consumer -> {
      // on success we set the handler that will store message in the database
      consumer.handler(message -> storeInDatabase(message.body()));
    }).subscribe(consumer -> {
      // complete the verticle start with a success
      future.complete();
    }, future::fail);
  }

  @Override
  public void stop(Future<Void> future) throws Exception {
    super.stop(future);
    sqlClient.close();
  }

  private void retrieveOperations(RoutingContext context) {
    // We retrieve the operation using the following process:
    // 1. Get the connection
    // 2. When done, execute the query
    // 3. When done, iterate over the result to build a list
    // 4. close the connection
    // 5. return this list in the response

    //TODO
    // ----
    Single<SQLConnection> connectionRetrieved = sqlClient.rxGetConnection();
    Single<ResultSet> resultSetSingle = connectionRetrieved.flatMap(connection ->
            connection.rxQuery(SELECT_STATEMENT).doAfterTerminate(connection::close));

    resultSetSingle.subscribe(resultSet -> {
            List<JsonObject> mappedResults = resultSet.getRows()
                    .stream()
                    .map(json -> new JsonObject(json.getString("operation")))
                    .collect(Collectors.toList());
            context.response().setStatusCode(200).end(Json.encodePrettily(mappedResults));
        },
        err -> context.response().setStatusCode(500).end(err.getMessage())
    );
    // ----
  }

  private Single<HttpServer> configureTheHTTPServer() {
    //TODO
    //----
      Router router = Router.router(vertx);
      router.get("/").handler(this::retrieveOperations);
      return vertx.createHttpServer()
              .requestHandler(router::accept)
              .rxListen(config().getInteger("http.port", 0));
  }

  private Single<MessageConsumer<JsonObject>> retrieveThePortfolioMessageSource() {
    return MessageSource.rxGetConsumer(discovery, new JsonObject().put("name", "portfolio-events"));
  }


  private void storeInDatabase(JsonObject operation) {
    // Storing in the database is also a multi step process,
    // 1. need to retrieve a connection
    // 2. execute the insertion statement
    // 3. close the connection
    // Step 1 get the connection
    Single<SQLConnection> connectionRetrieved = sqlClient.rxGetConnection();
    // Step 2, when the connection is retrieved (this may have failed), do the insertion (upon success)
    Single<UpdateResult> update = connectionRetrieved.flatMap(connection -> connection
        .rxUpdateWithParams(INSERT_STATEMENT, new JsonArray().add(operation.encode()))

        // Step 3, when the insertion is done, close the connection.
        .doAfterTerminate(connection::close));

    update.subscribe(result -> {
      // Ok
    }, err -> System.err.println("Failed to insert operation in database: " + err));
  }

  private Single<Void> initializeDatabase(boolean drop) {

    // The database initialization is a multi-step process:
    // 1. Retrieve the connection
    // 2. Drop the table is exist
    // 3. Create the table
    // 4. Close the connection (in any case)
    // To handle such a process, we are going to create an RxJava Single and compose it with the RxJava flatMap operation:
    // retrieve the connection -> drop table -> create table -> close the connection
    // For this we use `Func1<X, Single<R>>`that takes a parameter `X` and return a `Single<R>` object.

    // This is the starting point of our Rx operations
    // This single will be completed when the connection with the database is established.
    // We are going to use this single as a reference on the connection to close it.
    Single<SQLConnection> connectionRetrieved = sqlClient.rxGetConnection();

    // Ok, now it's time to chain all these actions:
    Single<Object> resultSingle = connectionRetrieved
        .flatMap(conn -> {
          // When the connection is retrieved

          // Prepare the batch
          List<Single<Void>> sqlBatch = new ArrayList<>();
          if (drop) {
            // When the table is dropped, we recreate it
            sqlBatch.add(conn.rxExecute(DROP_STATEMENT));
          }
          // Just create the table
          sqlBatch.add(conn.rxExecute(CREATE_TABLE_STATEMENT));

          // We compose with a statement batch
          //Single<List<Integer>> next = conn.rxExecute(batch);
            return Single.zip(sqlBatch, unused -> null).doAfterTerminate(conn::close);
          // Whatever the result, if the connection has been retrieved, close it
        });

    return resultSingle.map(list -> null);
  }
}
