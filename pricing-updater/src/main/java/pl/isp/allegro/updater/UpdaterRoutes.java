package pl.isp.allegro.updater;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import pl.isp.allegro.common.Endpoints;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.camel.HttpResilience;
import pl.isp.allegro.common.model.PriceChangeCommand;
import pl.isp.allegro.common.model.PriceChangedEvent;

import java.time.Instant;

/**
 * Aktualizator ofert: RabbitMQ (komendy) -> IDEMPOTENT CONSUMER -> THROTTLER -> PATCH w API Allegro
 * (RETRY + CIRCUIT BREAKER) -> potwierdzenie na topic Kafki price.changes (historia zmian cen).
 */
public class UpdaterRoutes extends RouteBuilder {

    private long throttlePerSecond = 2;
    private int failureThreshold = 5;
    private long failureWindowMs = 30000;
    private long halfOpenAfterMs = 15000;

    private String commandsUri = Endpoints.rabbitConsumer(Endpoints.QUEUE_COMMANDS, 1, 5);
    private String allegroUri = "{{allegro.api.url}}/sale/product-offers/${exchangeProperty.offerId}"
            + "?throwExceptionOnFailure=true&connectTimeout=3000&socketTimeout=5000";
    private String changesUri = Endpoints.kafkaProducer(Endpoints.TOPIC_PRICE_CHANGES);

    @Override
    public void configure() {
        HttpResilience.retryPolicy(this, "{{http.retry.max}}", "{{http.retry.delayMs}}", "{{http.retry.delay429Ms}}");

        from(commandsUri).routeId("updater")
                .routePolicy(HttpResilience.circuitBreaker(failureThreshold, failureWindowMs, halfOpenAfterMs))
                .process(exchange -> {
                    PriceChangeCommand cmd = Json.body(exchange, PriceChangeCommand.class);
                    if (cmd.commandId == null || cmd.offerId == null || cmd.newPrice == null) {
                        throw new IllegalArgumentException("Niekompletna komenda zmiany ceny");
                    }
                    exchange.setProperty("command", cmd);
                    exchange.setProperty("commandId", cmd.commandId);
                    exchange.setProperty("offerId", cmd.offerId);
                })
                // RabbitMQ gwarantuje at-least-once: ta sama komenda moze przyjsc dwa razy (np. po zerwaniu
                // polaczenia przed ack). Ceny nie wolno zmienic dwukrotnie -> EIP Idempotent Consumer.
                .idempotentConsumer(exchangeProperty("commandId"), MemoryIdempotentRepository.memoryIdempotentRepository(10000))
                .throttle(throttlePerSecond).timePeriodMillis(1000)
                .removeHeaders("*")
                .setHeader(Exchange.HTTP_METHOD, constant("PATCH"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/vnd.allegro.public.v1+json"))
                .setHeader("Authorization", simple("Bearer {{allegro.api.token}}"))
                .process(exchange -> {
                    PriceChangeCommand cmd = exchange.getProperty("command", PriceChangeCommand.class);
                    exchange.getIn().setBody("{\"sellingMode\":{\"price\":{\"amount\":\""
                            + cmd.newPrice.toPlainString() + "\",\"currency\":\"PLN\"}}}");
                })
                .toD(allegroUri)
                .process(exchange -> {
                    PriceChangeCommand cmd = exchange.getProperty("command", PriceChangeCommand.class);
                    PriceChangedEvent event = new PriceChangedEvent();
                    event.commandId = cmd.commandId;
                    event.productId = cmd.productId;
                    event.offerId = cmd.offerId;
                    event.oldPrice = cmd.oldPrice;
                    event.newPrice = cmd.newPrice;
                    event.reason = cmd.reason;
                    event.changedAt = Instant.now().toString();
                    exchange.setProperty("productId", cmd.productId);
                    exchange.getIn().setBody(Json.write(event));
                })
                .removeHeaders("*")
                .setHeader("kafka.KEY", exchangeProperty("productId"))
                .to(changesUri)
                .log("Aktualizator: oferta ${exchangeProperty.offerId} - cena zmieniona (${exchangeProperty.commandId})");
    }

    public void setThrottlePerSecond(long throttlePerSecond) {
        this.throttlePerSecond = throttlePerSecond;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
    }

    public void setFailureWindowMs(long failureWindowMs) {
        this.failureWindowMs = failureWindowMs;
    }

    public void setHalfOpenAfterMs(long halfOpenAfterMs) {
        this.halfOpenAfterMs = halfOpenAfterMs;
    }

    public void setCommandsUri(String commandsUri) {
        this.commandsUri = commandsUri;
    }

    public void setAllegroUri(String allegroUri) {
        this.allegroUri = allegroUri;
    }

    public void setChangesUri(String changesUri) {
        this.changesUri = changesUri;
    }
}
