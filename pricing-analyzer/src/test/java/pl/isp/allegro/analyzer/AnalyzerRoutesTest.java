package pl.isp.allegro.analyzer;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.model.PriceAlert;
import pl.isp.allegro.common.model.PriceChangeCommand;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test trasy analizatora z brokerami podmienionymi na endpointy direct:/mock:. */
class AnalyzerRoutesTest extends CamelTestSupport {

    @Override
    protected RoutesBuilder createRouteBuilder() {
        AnalyzerRoutes routes = new AnalyzerRoutes();
        routes.setOffersUri("direct:offers");
        routes.setStatsUri("mock:stats");
        routes.setCommandsUri("mock:commands");
        routes.setAlertUri("mock:alerts");
        routes.setDigestUri("mock:digest");
        routes.setDeadLetterUri("mock:dlt");
        routes.setDigestIntervalMs(300);
        return routes;
    }

    /** koszt 100, marza 10% => cena minimalna 110; nasza cena 130 */
    private static String event(String eventId, String competitorPrice, Instant collectedAt) {
        return "{\"eventId\":\"" + eventId + "\",\"collectedAt\":\"" + collectedAt + "\","
                + "\"task\":{\"taskId\":\"" + eventId + "\",\"productId\":\"P1\",\"name\":\"Produkt\",\"ourOfferId\":\"OUR\","
                + "\"purchaseCost\":100,\"minMarginPct\":10},"
                + "\"listing\":{\"items\":{\"promoted\":[],\"regular\":["
                + offer("OUR", "my", "130.00") + "," + offer("C1", "konkurent", competitorPrice) + "]}}}";
    }

    private static String offer(String id, String seller, String price) {
        return "{\"id\":\"" + id + "\",\"seller\":{\"login\":\"" + seller + "\"},"
                + "\"sellingMode\":{\"price\":{\"amount\":\"" + price + "\",\"currency\":\"PLN\"}}}";
    }

    private MockEndpoint mock(String name, int count) {
        MockEndpoint m = getMockEndpoint("mock:" + name);
        m.expectedMessageCount(count);
        return m;
    }

    @Test
    void tanszyKonkurent_komendaNaRabbit() throws Exception {
        MockEndpoint commands = mock("commands", 1);
        mock("alerts", 0);
        mock("stats", 1);
        template.sendBody("direct:offers", event("e1", "120.00", Instant.now()));
        MockEndpoint.assertIsSatisfied(context);

        PriceChangeCommand cmd = Json.read(commands.getExchanges().get(0).getIn().getBody(String.class), PriceChangeCommand.class);
        assertEquals("cmd-e1", cmd.commandId);
        assertEquals("OUR", cmd.offerId);
        assertEquals(0, new BigDecimal("119.99").compareTo(cmd.newPrice));
    }

    @Test
    void konkurentPonizejMarzy_alertMqttIPodsumowanie() throws Exception {
        mock("commands", 0);
        MockEndpoint alerts = mock("alerts", 1);
        MockEndpoint digest = mock("digest", 1);
        template.sendBody("direct:offers", event("e2", "105.00", Instant.now()));
        MockEndpoint.assertIsSatisfied(context);

        assertEquals("alerts/price/P1", alerts.getExchanges().get(0).getIn().getHeader("CamelPahoOverrideTopic"));
        PriceAlert alert = Json.read(alerts.getExchanges().get(0).getIn().getBody(String.class), PriceAlert.class);
        assertEquals("konkurent", alert.competitorSeller);
        assertTrue(digest.getExchanges().get(0).getIn().getBody(String.class).contains("\"alertCount\":1"));
    }

    @Test
    void duplikatZdarzenia_przetwarzanyRaz() throws Exception {
        mock("commands", 1);
        mock("stats", 1);
        String event = event("e3", "120.00", Instant.now());
        template.sendBody("direct:offers", event);
        template.sendBody("direct:offers", event);
        MockEndpoint.assertIsSatisfied(context);
    }

    @Test
    void przeterminowaneZdarzenie_tylkoStatystyki() throws Exception {
        mock("commands", 0);
        mock("alerts", 0);
        mock("stats", 1);
        template.sendBody("direct:offers", event("e4", "120.00", Instant.now().minusSeconds(600)));
        MockEndpoint.assertIsSatisfied(context);
    }

    @Test
    void niepoprawnyKomunikat_deadLetterChannel() throws Exception {
        MockEndpoint dlt = mock("dlt", 1);
        mock("stats", 0);
        template.sendBody("direct:offers", "to nie jest JSON");
        MockEndpoint.assertIsSatisfied(context);
        // useOriginalMessage: na DLT trafia oryginalna tresc
        assertEquals("to nie jest JSON", dlt.getExchanges().get(0).getIn().getBody(String.class));
    }
}
