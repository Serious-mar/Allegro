package pl.isp.allegro.collector;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import pl.isp.allegro.common.Endpoints;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.camel.HttpResilience;
import pl.isp.allegro.common.model.MarketOffersEvent;
import pl.isp.allegro.common.model.ProductTask;

import java.time.Instant;

/**
 * Kolektor ofert: RabbitMQ (zadania) -> THROTTLER -> API Allegro (RETRY + CIRCUIT BREAKER) -> Kafka market.offers.
 * Zadanie jest potwierdzane (ack) dopiero po zapisaniu zdarzenia w Kafce; po wyczerpaniu ponowien
 * trafia do kolejki offer.check.tasks.dlq.
 */
public class CollectorRoutes extends RouteBuilder {

    private long throttlePerSecond = 5;
    private int failureThreshold = 5;
    private long failureWindowMs = 30000;
    private long halfOpenAfterMs = 15000;

    private String tasksUri = Endpoints.rabbitConsumer(Endpoints.QUEUE_TASKS, 2, 10);
    private String allegroUri = "{{allegro.api.url}}/offers/listing?throwExceptionOnFailure=true&connectTimeout=3000&socketTimeout=5000";
    private String offersUri = Endpoints.kafkaProducer(Endpoints.TOPIC_OFFERS);

    @Override
    public void configure() {
        HttpResilience.retryPolicy(this, "{{http.retry.max}}", "{{http.retry.delayMs}}", "{{http.retry.delay429Ms}}");

        from(tasksUri).routeId("collector")
                .routePolicy(HttpResilience.circuitBreaker(failureThreshold, failureWindowMs, halfOpenAfterMs))
                .process(exchange -> {
                    ProductTask task = Json.body(exchange, ProductTask.class);
                    if (task.productId == null || task.taskId == null) {
                        throw new IllegalArgumentException("Zadanie bez productId/taskId");
                    }
                    exchange.setProperty("task", task);
                    exchange.setProperty("productId", task.productId);
                })
                // EIP Throttler: nie wiecej niz N zapytan na sekunde do API Allegro
                .throttle(throttlePerSecond).timePeriodMillis(1000)
                .removeHeaders("*")
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .setHeader(Exchange.HTTP_QUERY, simple("phrase=${exchangeProperty.productId}"))
                .setHeader("Authorization", simple("Bearer {{allegro.api.token}}"))
                .setHeader("Accept", constant("application/vnd.allegro.public.v1+json"))
                .setBody(constant(null))
                .to(allegroUri)
                .process(exchange -> {
                    ProductTask task = exchange.getProperty("task", ProductTask.class);
                    MarketOffersEvent event = new MarketOffersEvent();
                    event.eventId = task.taskId;
                    event.task = task;
                    event.collectedAt = Instant.now().toString();
                    event.listing = Json.tree(exchange.getIn().getBody(String.class));
                    exchange.getIn().setBody(Json.write(event));
                })
                .removeHeaders("*")
                // klucz = productId: wszystkie zdarzenia produktu w jednej partycji, w kolejnosci
                .setHeader("kafka.KEY", exchangeProperty("productId"))
                .to(offersUri)
                .log("Kolektor: ${exchangeProperty.productId} -> " + Endpoints.TOPIC_OFFERS);
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

    public void setTasksUri(String tasksUri) {
        this.tasksUri = tasksUri;
    }

    public void setAllegroUri(String allegroUri) {
        this.allegroUri = allegroUri;
    }

    public void setOffersUri(String offersUri) {
        this.offersUri = offersUri;
    }
}
