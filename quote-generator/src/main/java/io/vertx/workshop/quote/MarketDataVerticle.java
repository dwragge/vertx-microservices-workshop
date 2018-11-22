package io.vertx.workshop.quote;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.Console;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A verticle simulating the evaluation of a company evaluation in a very unrealistic and irrational way.
 * It emits the new data on the `market` address on the event bus.
 */
public class MarketDataVerticle extends AbstractVerticle {
    private String name;
    private static final double dt = 0.1;
    private int variation;
    private long period;
    private String symbol;
    int stocks;

    private double price;
    private double value;
    double bid;
    double ask;

    int share;

    // Brownian motion variables
    private double mu = 0.00001;
    private double sigma = 0.0001;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
    * Method called when the verticle is deployed.
    */
    @Override
    public void start() {
        // Retrieve the configuration, and initialize the verticle.
        JsonObject config = config();
        init(config);

        // Every `period` ms, the given Handler is called.
        vertx.setPeriodic(period, l -> {
          compute();
          send();
        });
    }

    /**
    * Read the configuration and set the initial values.
    * @param config the configuration
    */
    void init(JsonObject config) {
        period = config.getLong("period", 1000L);
        variation = config.getInteger("variation", 100);
        name = config.getString("name");
        Objects.requireNonNull(name);
        symbol = config.getString("symbol", name);
        stocks = config.getInteger("volume", 10000);
        price = config.getDouble("price", 100.0);
        value = price;

        bid = value - random.nextDouble(value / 100);
        ask = value + random.nextDouble(value / 100);

        share = stocks / 2;

        System.out.println(Json.encodePrettily(toJson()));
    }

    /**
    * Sends the market data on the event bus.
    */
    private void send() {
        vertx.eventBus().publish(GeneratorConfigVerticle.ADDRESS, toJson());
    }

    private double W(int n, long t) {
        double sum = 0;
        for (int k = 0; k <= n * t; k++) {
            sum += random.nextGaussian();
        }
        return sum / Math.sqrt(n);
    }

    /**
    * Compute the new evaluation...
    */
    void compute() {
        value = value * Math.exp((mu - 0.5 * sigma * sigma ) * dt + (sigma * value * random.nextGaussian() * Math.sqrt(dt)));
        bid = value - random.nextDouble( 0.01, 0.2);
        ask = value + random.nextDouble( 0.01, 0.2);

        // round off
        value = BigDecimal.valueOf(value).setScale(4, BigDecimal.ROUND_UP).doubleValue();
        bid = BigDecimal.valueOf(bid).setScale(4, BigDecimal.ROUND_UP).doubleValue();
        ask = BigDecimal.valueOf(ask).setScale(4, BigDecimal.ROUND_UP).doubleValue();

        if (value <= 0.1) {
            value = 1;
        }
        if (ask <= 0.1) {
            ask = 1;
        }
        if (bid <= 0.1) {
            bid = 1;
        }

        if (random.nextBoolean()) {
          // Adjust share
          int shareVariation = random.nextInt(100);
          if (shareVariation > 0 && share + shareVariation < stocks) {
            share += shareVariation;
          } else if (shareVariation < 0 && share + shareVariation > 0) {
            share += shareVariation;
          }
        }
    }

    /**
    * @return a json representation of the market data (quote). The structure is close to
    * <a href="https://en.wikipedia.org/wiki/Market_data">https://en.wikipedia.org/wiki/Market_data</a>.
    */
    private JsonObject toJson() {
        return new JsonObject()
            .put("exchange", "vert.x stock exchange")
            .put("symbol", symbol)
            .put("name", name)
            .put("bid", bid)
            .put("ask", ask)
            .put("volume", stocks)
            .put("open", price)
            .put("shares", share);
    }
}
