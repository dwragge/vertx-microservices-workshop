package io.vertx.workshop.trader.impl;

import com.sun.tools.javac.util.List;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.servicediscovery.types.MessageSource;
import io.vertx.workshop.common.MicroServiceVerticle;
import io.vertx.workshop.portfolio.PortfolioService;

/**
 * A compulsive trader...
 */
public class JavaCompulsiveTraderVerticle extends MicroServiceVerticle {

  @Override
  public void start(Future<Void> future) {
    super.start();

    String company = TraderUtils.pickACompany();
    int numShares = TraderUtils.pickANumber();
    System.out.println("Java compulsive trader configured for company " + company + " and shares: " + numShares);

    Future<MessageConsumer<JsonObject>> marketConsumerFuture = Future.future();
    Future<PortfolioService> portfolioServiceFuture = Future.future();

    MessageSource.getConsumer(discovery, new JsonObject().put("name", "market-data"), marketConsumerFuture.completer());
    EventBusService.getProxy(discovery, PortfolioService.class, portfolioServiceFuture.completer());

    CompositeFuture.all(List.of(marketConsumerFuture, portfolioServiceFuture)).setHandler(ar -> {
       if (ar.failed()) {
           future.fail(ar.cause());
       } else {
           PortfolioService portfolioService = portfolioServiceFuture.result();
           MessageConsumer<JsonObject> messageConsumer = marketConsumerFuture.result();

           messageConsumer.handler(message -> {
               JsonObject quote = message.body();
               TraderUtils.dumbTradingLogic(company, numShares, portfolioService, quote);
           });

           future.complete();
       }
    });
  }


}
