package pl.isp.allegro.analyzer;

import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import pl.isp.allegro.common.Endpoints;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.model.Decision;
import pl.isp.allegro.common.model.MarketOffersEvent;
import pl.isp.allegro.common.model.MarketStats;
import pl.isp.allegro.common.model.PriceAlert;
import pl.isp.allegro.common.model.PriceChangeCommand;
import pl.isp.allegro.common.pricing.PricingEngine;
import pl.isp.allegro.common.pricing.PricingRules;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Analizator i silnik regul:
 * Kafka market.offers -> IDEMPOTENT CONSUMER -> statystyki -> Kafka market.stats
 *   -> CONTENT-BASED ROUTER: zmiana ceny -> RabbitMQ | alert -> MQTT (QoS 1) | brak akcji
 * Alerty sa dodatkowo zbierane przez AGGREGATOR w okresowe podsumowanie (MQTT QoS 0, retained).
 * Zdarzenia, ktorych nie da sie przetworzyc, trafiaja przez DEAD LETTER CHANNEL na topic market.offers.dlt.
 */
public class AnalyzerRoutes extends RouteBuilder {

    private PricingRules rules = new PricingRules();
    private long digestIntervalMs = 30000;
    private long maxEventAgeMs = 60000;

    private String offersUri = Endpoints.kafkaConsumer(Endpoints.TOPIC_OFFERS, "pricing-analyzer");
    private String statsUri = Endpoints.kafkaProducer(Endpoints.TOPIC_STATS);
    private String deadLetterUri = Endpoints.kafkaProducer(Endpoints.TOPIC_OFFERS_DLT);
    private String commandsUri = Endpoints.rabbitProducer(Endpoints.QUEUE_COMMANDS);
    // QoS 1 (at least once): alert musi dotrzec, duplikat jest akceptowalny
    private String alertUri = Endpoints.mqttProducer("alerts/price", 1, false, "pricing-analyzer-alerts");
    // QoS 0 (at most once) + retained: podsumowanie jest okresowe, zgubione zastapi nastepne,
    // a nowy subskrybent od razu dostaje ostatnie
    private String digestUri = Endpoints.mqttProducer(Endpoints.MQTT_DIGEST_TOPIC, 0, true, "pricing-analyzer-digest");

    @Override
    public void configure() {
        PricingEngine engine = new PricingEngine(rules);

        // EIP Dead Letter Channel: 2 ponowienia (np. chwilowa awaria RabbitMQ/MQTT), potem oryginalny
        // komunikat laduje na dead letter topicu i nie blokuje partycji ("poison message").
        errorHandler(deadLetterChannel(deadLetterUri)
                .maximumRedeliveries(2).redeliveryDelay(1000)
                .useOriginalMessage()
                .logExhaustedMessageHistory(false).logHandled(true)
                .retriesExhaustedLogLevel(org.apache.camel.LoggingLevel.WARN)
                .onPrepareFailure(exchange -> {
                    Exception cause = exchange.getProperty(org.apache.camel.Exchange.EXCEPTION_CAUGHT, Exception.class);
                    exchange.getIn().setHeader("dlt.reason", cause == null ? "?" : String.valueOf(cause.getMessage()));
                }));

        from(offersUri).routeId("analyzer")
                .process(exchange -> {
                    MarketOffersEvent event = Json.body(exchange, MarketOffersEvent.class);
                    if (event.eventId == null) {
                        throw new IllegalArgumentException("Zdarzenie bez eventId");
                    }
                    exchange.setProperty("event", event);
                    exchange.setProperty("eventId", event.eventId);
                })
                // Kafka dostarcza at-least-once (rebalans, ponowienie producenta) -> odsiewamy duplikaty po eventId
                .idempotentConsumer(exchangeProperty("eventId"), MemoryIdempotentRepository.memoryIdempotentRepository(10000))
                .process(exchange -> {
                    MarketOffersEvent event = exchange.getProperty("event", MarketOffersEvent.class);
                    MarketStats stats = engine.analyze(event);
                    // Nadrabianie zaleglosci z Kafki (np. po restarcie): statystyki trafiaja do historii,
                    // ale na podstawie nieaktualnych cen nie wolno juz podejmowac akcji.
                    if (stats.decision != Decision.HOLD && isStale(event.collectedAt)) {
                        stats.decision = Decision.HOLD;
                        stats.targetPrice = null;
                        stats.reason = "Zdarzenie przeterminowane - bez akcji";
                    }
                    exchange.setProperty("stats", stats);
                    exchange.setProperty("decision", stats.decision.name());
                    exchange.setProperty("productId", stats.productId);
                    exchange.setProperty("summary", stats.productId + ": " + stats.ourPrice
                            + (stats.targetPrice == null ? "" : " -> " + stats.targetPrice) + " (" + stats.reason + ")");
                    exchange.getIn().setBody(Json.write(stats));
                })
                .removeHeaders("*")
                .setHeader("kafka.KEY", exchangeProperty("productId"))
                .to(statsUri)
                .removeHeaders("*")
                // EIP Content-Based Router: routing wg decyzji silnika regul
                .choice()
                    .when(exchangeProperty("decision").isEqualTo(Decision.CHANGE_PRICE.name()))
                        .process(exchange -> {
                            MarketStats s = exchange.getProperty("stats", MarketStats.class);
                            PriceChangeCommand cmd = new PriceChangeCommand();
                            // deterministyczne ID: ponowne przetworzenie zdarzenia da te sama komende
                            cmd.commandId = "cmd-" + s.eventId;
                            cmd.productId = s.productId;
                            cmd.offerId = s.ourOfferId;
                            cmd.oldPrice = s.ourPrice;
                            cmd.newPrice = s.targetPrice;
                            cmd.reason = s.reason;
                            cmd.createdAt = Instant.now().toString();
                            exchange.getIn().setBody(Json.write(cmd));
                        })
                        .setHeader("CamelRabbitmqDeliveryMode", constant(2))
                        .setHeader("CamelRabbitmqContentType", constant("application/json"))
                        .to(commandsUri)
                        .log("Analizator: zmiana ceny ${exchangeProperty.summary}")
                    .when(exchangeProperty("decision").isEqualTo(Decision.ALERT.name()))
                        .process(exchange -> {
                            MarketStats s = exchange.getProperty("stats", MarketStats.class);
                            PriceAlert alert = new PriceAlert();
                            alert.alertId = UUID.randomUUID().toString();
                            alert.productId = s.productId;
                            alert.name = s.name;
                            alert.ourPrice = s.ourPrice;
                            alert.minAllowedPrice = s.minAllowedPrice;
                            alert.competitorPrice = s.competitorMin;
                            alert.competitorSeller = s.competitorMinSeller;
                            alert.message = s.reason;
                            alert.at = Instant.now().toString();
                            exchange.setProperty("alert", alert);
                            exchange.getIn().setBody(Json.write(alert));
                        })
                        .setHeader("CamelPahoOverrideTopic", simple(Endpoints.MQTT_ALERT_TOPIC_PREFIX + "${exchangeProperty.productId}"))
                        .to(alertUri)
                        .log("Analizator: ALERT ${exchangeProperty.summary}")
                        .removeHeaders("*")
                        .setBody(exchangeProperty("alert"))
                        .to("seda:alert-digest")
                    .otherwise()
                        .log(org.apache.camel.LoggingLevel.DEBUG, "Analizator: bez zmian ${exchangeProperty.summary}")
                .end();

        // EIP Aggregator: wszystkie alerty z okna czasowego -> jedno podsumowanie
        from("seda:alert-digest").routeId("alert-digest")
                .aggregate(constant(true), AggregationStrategies.groupedBody())
                    .completionInterval(digestIntervalMs)
                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<PriceAlert> alerts = exchange.getIn().getBody(List.class);
                    Map<String, Object> digest = new LinkedHashMap<>();
                    TreeSet<String> products = new TreeSet<>();
                    alerts.forEach(a -> products.add(a.productId));
                    digest.put("at", Instant.now().toString());
                    digest.put("windowMs", digestIntervalMs);
                    digest.put("alertCount", alerts.size());
                    digest.put("products", products);
                    exchange.getIn().setBody(Json.write(digest));
                })
                .to(digestUri)
                .log("Analizator: podsumowanie alertow ${body}");
    }

    private boolean isStale(String collectedAt) {
        try {
            return java.time.Duration.between(Instant.parse(collectedAt), Instant.now()).toMillis() > maxEventAgeMs;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public void setMaxEventAgeMs(long maxEventAgeMs) {
        this.maxEventAgeMs = maxEventAgeMs;
    }

    public void setRules(PricingRules rules) {
        this.rules = rules;
    }

    public void setDigestIntervalMs(long digestIntervalMs) {
        this.digestIntervalMs = digestIntervalMs;
    }

    public void setOffersUri(String offersUri) {
        this.offersUri = offersUri;
    }

    public void setStatsUri(String statsUri) {
        this.statsUri = statsUri;
    }

    public void setDeadLetterUri(String deadLetterUri) {
        this.deadLetterUri = deadLetterUri;
    }

    public void setCommandsUri(String commandsUri) {
        this.commandsUri = commandsUri;
    }

    public void setAlertUri(String alertUri) {
        this.alertUri = alertUri;
    }

    public void setDigestUri(String digestUri) {
        this.digestUri = digestUri;
    }
}
