package pl.isp.allegro.collector;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.model.MarketOffersEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test polityki retry kolektora: API Allegro zastapione trasa direct:, ktora zwraca zadane kody HTTP. */
class CollectorRoutesTest extends CamelTestSupport {

    private static final String TASK = "{\"taskId\":\"t1\",\"productId\":\"P1\",\"ourOfferId\":\"OUR\",\"purchaseCost\":100,\"minMarginPct\":10}";

    private final Deque<Integer> responses = new ArrayDeque<>();
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    protected Properties useOverridePropertiesWithPropertiesComponent() {
        Properties p = new Properties();
        p.setProperty("http.retry.max", "3");
        p.setProperty("http.retry.delayMs", "10");
        p.setProperty("http.retry.delay429Ms", "10");
        p.setProperty("allegro.api.token", "test");
        return p;
    }

    @Override
    protected RoutesBuilder[] createRouteBuilders() {
        CollectorRoutes routes = new CollectorRoutes();
        routes.setTasksUri("direct:tasks");
        routes.setAllegroUri("direct:allegro");
        routes.setOffersUri("mock:offers");
        routes.setFailureThreshold(100);
        return new RoutesBuilder[] {routes, new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:allegro").errorHandler(noErrorHandler()).process(e -> {
                    calls.incrementAndGet();
                    Integer status = responses.poll();
                    if (status != null && status != 200) {
                        throw new HttpOperationFailedException("http://allegro/offers/listing", status, "blad", null, null, null);
                    }
                    e.getMessage().setBody("{\"items\":{\"promoted\":[],\"regular\":[]}}");
                });
            }
        }};
    }

    @Test
    void bledy429i500SaPonawianeAzDoSukcesu() throws Exception {
        responses.addAll(List.of(429, 500, 200));
        MockEndpoint offers = getMockEndpoint("mock:offers");
        offers.expectedMessageCount(1);
        offers.expectedHeaderReceived("kafka.KEY", "P1");

        template.sendBody("direct:tasks", TASK);

        MockEndpoint.assertIsSatisfied(context);
        assertEquals(3, calls.get());
        MarketOffersEvent event = Json.read(offers.getExchanges().get(0).getIn().getBody(String.class), MarketOffersEvent.class);
        assertEquals("t1", event.eventId);
        assertEquals("P1", event.task.productId);
    }

    @Test
    void poWyczerpaniuPonowienWyjatekWracaDoKonsumenta() {
        responses.addAll(List.of(500, 500, 500, 500, 500));
        // nieobsluzony wyjatek = nack w RabbitMQ = komunikat trafia do DLQ
        assertThrows(CamelExecutionException.class, () -> template.sendBody("direct:tasks", TASK));
        assertEquals(4, calls.get(), "1 proba + 3 ponowienia");
    }

    @Test
    void blad4xxNieJestPonawiany() {
        responses.add(404);
        assertThrows(CamelExecutionException.class, () -> template.sendBody("direct:tasks", TASK));
        assertEquals(1, calls.get());
    }
}
