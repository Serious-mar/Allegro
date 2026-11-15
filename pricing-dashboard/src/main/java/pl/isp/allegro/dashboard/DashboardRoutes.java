package pl.isp.allegro.dashboard;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import pl.isp.allegro.common.Endpoints;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Dashboard: odczyt historii z Kafki (market.stats, price.changes) i alertow z MQTT,
 * prosta strona WWW + JSON API wystawione przez camel-netty-http.
 */
public class DashboardRoutes extends RouteBuilder {

    private DashboardState state;

    @Override
    public void configure() throws Exception {
        String page;
        try (InputStream in = DashboardRoutes.class.getResourceAsStream("/web/index.html")) {
            page = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        // seekTo=beginning: po kazdym starcie czytamy topic od poczatku - Kafka jest tu trwalym logiem
        // zdarzen, z ktorego dowolny nowy konsument moze odtworzyc pelna historie cen.
        from(Endpoints.kafkaConsumer(Endpoints.TOPIC_STATS, "pricing-dashboard") + "&seekTo=beginning")
                .routeId("dashboard-stats")
                .bean(state, "onStats(${body})");

        from(Endpoints.kafkaConsumer(Endpoints.TOPIC_PRICE_CHANGES, "pricing-dashboard") + "&seekTo=beginning")
                .routeId("dashboard-price-changes")
                .bean(state, "onPriceChange(${body})");

        // MQTT: wildcard '+' = alerty wszystkich produktow; QoS 1 i trwala sesja (cleanSession=false)
        from(Endpoints.mqttConsumer(Endpoints.MQTT_ALERT_TOPIC_PREFIX + "+", 1, "pricing-dashboard-alerts"))
                .routeId("dashboard-alerts")
                .convertBodyTo(String.class)
                .bean(state, "onAlert(${body})");

        from(Endpoints.mqttConsumer(Endpoints.MQTT_DIGEST_TOPIC, 0, "pricing-dashboard-digest"))
                .routeId("dashboard-digest")
                .convertBodyTo(String.class)
                .bean(state, "onDigest(${body})");

        from("netty-http:http://0.0.0.0:{{dashboard.port}}?matchOnUriPrefix=true&httpMethodRestrict=GET")
                .routeId("dashboard-http")
                .choice()
                    .when(header(Exchange.HTTP_URI).startsWith("/api/state"))
                        .setHeader(Exchange.CONTENT_TYPE, constant("application/json; charset=utf-8"))
                        .setHeader("Cache-Control", constant("no-store"))
                        .setBody(method(state, "snapshot"))
                    .otherwise()
                        .setHeader(Exchange.CONTENT_TYPE, constant("text/html; charset=utf-8"))
                        .setBody(constant(page))
                .end();
    }

    public void setState(DashboardState state) {
        this.state = state;
    }
}
