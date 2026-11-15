package pl.isp.allegro.mock;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import pl.isp.allegro.common.Json;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Mock REST API Allegro wystawiony przez camel-netty-http:
 *   GET   /offers/listing?phrase={productId}   - lista ofert produktu
 *   PATCH /sale/product-offers/{offerId}       - zmiana ceny oferty
 * Oba endpointy losowo zwracaja 429 (z naglowkiem Retry-After) oraz 500.
 */
public class AllegroMockRoutes extends RouteBuilder {

    private MarketSimulator market;
    private double error429Rate = 0.15;
    private double error500Rate = 0.10;

    @Override
    public void configure() {
        String base = "netty-http:http://0.0.0.0:{{mock.port}}";

        from(base + "/offers/listing?httpMethodRestrict=GET").routeId("mock-listing")
                .process(exchange -> {
                    if (injectFailure(exchange)) {
                        return;
                    }
                    String productId = exchange.getIn().getHeader("phrase", String.class);
                    if (productId == null || productId.isEmpty()) {
                        respond(exchange, 400, error("VALIDATION_ERROR", "Brak parametru phrase"));
                        return;
                    }
                    respond(exchange, 200, market.listing(productId));
                });

        from(base + "/sale/product-offers?matchOnUriPrefix=true&httpMethodRestrict=PATCH").routeId("mock-update-price")
                .process(exchange -> {
                    if (injectFailure(exchange)) {
                        return;
                    }
                    String path = exchange.getIn().getHeader(Exchange.HTTP_URI, String.class);
                    String offerId = path.substring(path.lastIndexOf('/') + 1);
                    JsonNode amount = Json.tree(exchange.getIn().getBody(String.class))
                            .path("sellingMode").path("price").path("amount");
                    if (amount.isMissingNode()) {
                        respond(exchange, 422, error("VALIDATION_ERROR", "Brak sellingMode.price.amount"));
                        return;
                    }
                    String updated = market.updatePrice(offerId, new BigDecimal(amount.asText()));
                    if (updated == null) {
                        respond(exchange, 404, error("NOT_FOUND", "Nie ma oferty " + offerId));
                    } else {
                        log.info("Mock Allegro: oferta {} ma teraz cene {}", offerId, amount.asText());
                        respond(exchange, 200, updated);
                    }
                });
    }

    private boolean injectFailure(Exchange exchange) {
        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < error429Rate) {
            exchange.getMessage().setHeader("Retry-After", "1");
            respond(exchange, 429, error("TOO_MANY_REQUESTS", "Przekroczono limit zapytan"));
            return true;
        }
        if (roll < error429Rate + error500Rate) {
            respond(exchange, 500, error("INTERNAL_SERVER_ERROR", "Losowy blad serwera"));
            return true;
        }
        return false;
    }

    private static void respond(Exchange exchange, int status, String json) {
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, status);
        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/vnd.allegro.public.v1+json");
        exchange.getMessage().setBody(json);
    }

    private static String error(String code, String message) {
        return "{\"errors\":[{\"code\":\"" + code + "\",\"message\":\"" + message + "\"}]}";
    }

    public void setMarket(MarketSimulator market) {
        this.market = market;
    }

    public void setError429Rate(double error429Rate) {
        this.error429Rate = error429Rate;
    }

    public void setError500Rate(double error500Rate) {
        this.error500Rate = error500Rate;
    }
}
