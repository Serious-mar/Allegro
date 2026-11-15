package pl.isp.allegro.common.camel;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueueCircuitBreakerTest extends CamelTestSupport {

    private final QueueCircuitBreaker breaker = new QueueCircuitBreaker(3, 10_000, 400);
    private final AtomicBoolean apiDown = new AtomicBoolean(true);

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:in").routeId("guarded").routePolicy(breaker)
                        .process(e -> {
                            if (apiDown.get()) {
                                throw new IllegalStateException("API niedostepne");
                            }
                        })
                        .to("mock:out");
            }
        };
    }

    private void send() {
        template.sendBody("direct:in", "x");
    }

    private void awaitState(QueueCircuitBreaker.State expected) throws InterruptedException {
        for (int i = 0; i < 100 && breaker.getState() != expected; i++) {
            Thread.sleep(50);
        }
        assertEquals(expected, breaker.getState());
    }

    @Test
    void otwieraSiePoProguBledowIZamykaPoUdanejProbie() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertThrows(CamelExecutionException.class, this::send);
        }
        assertEquals(QueueCircuitBreaker.State.OPEN, breaker.getState());

        // API dalej lezy: proba w HALF_OPEN ponownie otwiera obwod
        awaitState(QueueCircuitBreaker.State.HALF_OPEN);
        assertThrows(CamelExecutionException.class, this::send);
        assertEquals(QueueCircuitBreaker.State.OPEN, breaker.getState());

        // API wraca: pierwsza udana wymiana w HALF_OPEN zamyka obwod
        apiDown.set(false);
        awaitState(QueueCircuitBreaker.State.HALF_OPEN);
        getMockEndpoint("mock:out").expectedMessageCount(1);
        send();
        assertEquals(QueueCircuitBreaker.State.CLOSED, breaker.getState());
        org.apache.camel.component.mock.MockEndpoint.assertIsSatisfied(context);
    }

    @Test
    void komunikatOtwierajacyObwodWracaDoKolejki() {
        assertThrows(CamelExecutionException.class, this::send);
        org.apache.camel.Exchange second = template.send("direct:in", e -> e.getIn().setBody("x"));
        assertEquals(null, second.getMessage().getHeader(QueueCircuitBreaker.REQUEUE_HEADER), "ponizej progu -> DLQ");
        org.apache.camel.Exchange third = template.send("direct:in", e -> e.getIn().setBody("x"));
        assertEquals(true, third.getMessage().getHeader(QueueCircuitBreaker.REQUEUE_HEADER));
    }

    @Test
    void pojedynczeBledyNieOtwierajaObwodu() {
        assertThrows(CamelExecutionException.class, this::send);
        assertThrows(CamelExecutionException.class, this::send);
        assertEquals(QueueCircuitBreaker.State.CLOSED, breaker.getState());
    }
}
