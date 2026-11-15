package pl.isp.allegro.common.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Circuit Breaker dla trasy konsumujacej z kolejki (RoutePolicy).
 *
 * CLOSED    - normalna praca; zliczane sa nieudane wymiany w przesuwnym oknie czasowym.
 * OPEN      - prog bledow przekroczony: konsument kolejki jest WSTRZYMANY, a komunikat, na ktorym
 *             obwod sie otworzyl, wraca do kolejki (requeue). Zadania czekaja bezpiecznie w brokerze
 *             zamiast przepalac ponowienia i ladowac w DLQ.
 * HALF_OPEN - po czasie halfOpenAfter konsument rusza na probe: pierwsza zakonczona wymiana
 *             decyduje - sukces zamyka obwod, blad otwiera go ponownie.
 *
 * Dlaczego wlasna klasa, a nie ThrottlingExceptionRoutePolicy z Camela: tamta zatrzymuje konsumenta
 * w watku, ktory wlasnie obsluguje komunikat. Konsument RabbitMQ zamyka wtedy kanal AMQP z jego
 * wlasnego watku dostarczania i sie zakleszcza. Tutaj stop/start wykonuje osobny watek.
 * (camel-resilience4j nie ma feature'a dla Karafa w Camelu 3.x; poza tym wzorzec "wstrzymaj konsumenta"
 * lepiej pasuje do kolejki niz fallback pojedynczego wywolania.)
 */
public class QueueCircuitBreaker extends RoutePolicySupport {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    static final String REQUEUE_HEADER = "CamelRabbitmqRequeue";

    private static final Logger LOG = LoggerFactory.getLogger(QueueCircuitBreaker.class);

    private final int failureThreshold;
    private final long failureWindowMs;
    private final long halfOpenAfterMs;

    private final Predicate<Throwable> countsAsFailure;

    private final Deque<Long> failures = new ArrayDeque<>();
    private State state = State.CLOSED;
    private ScheduledExecutorService executor;

    public QueueCircuitBreaker(int failureThreshold, long failureWindowMs, long halfOpenAfterMs) {
        this(failureThreshold, failureWindowMs, halfOpenAfterMs, t -> true);
    }

    /**
     * @param countsAsFailure ktore wyjatki swiadcza o awarii uslugi (a nie o wadliwym komunikacie);
     *                        tylko one sa zliczane i tylko po nich komunikat wraca do kolejki
     */
    public QueueCircuitBreaker(int failureThreshold, long failureWindowMs, long halfOpenAfterMs,
                               Predicate<Throwable> countsAsFailure) {
        this.countsAsFailure = countsAsFailure;
        this.failureThreshold = failureThreshold;
        this.failureWindowMs = failureWindowMs;
        this.halfOpenAfterMs = halfOpenAfterMs;
    }

    @Override
    public void onInit(Route route) {
        // pula zarzadzana przez CamelContext - zamykana razem z nim
        executor = route.getCamelContext().getExecutorServiceManager()
                .newSingleThreadScheduledExecutor(this, "CircuitBreaker-" + route.getId());
    }

    @Override
    public synchronized void onExchangeDone(Route route, Exchange exchange) {
        boolean failed = exchange.getException() != null && countsAsFailure.test(exchange.getException());
        switch (state) {
            case CLOSED:
                if (failed) {
                    long now = System.currentTimeMillis();
                    failures.addLast(now);
                    while (!failures.isEmpty() && now - failures.peekFirst() > failureWindowMs) {
                        failures.removeFirst();
                    }
                    if (failures.size() >= failureThreshold) {
                        open(route, failures.size() + " bledow w ciagu " + failureWindowMs + " ms");
                    }
                }
                break;
            case HALF_OPEN:
                if (failed) {
                    open(route, "proba w stanie HALF_OPEN nieudana");
                } else {
                    state = State.CLOSED;
                    failures.clear();
                    LOG.info("Circuit breaker [{}]: CLOSED - usluga znow odpowiada", route.getId());
                }
                break;
            default:
                // OPEN: wymiany rozpoczete przed otwarciem obwodu - nie zmieniaja stanu
                break;
        }
        if (failed && state == State.OPEN) {
            // Usluga lezy, komunikat jest dobry: zamiast do DLQ ma wrocic do kolejki i poczekac
            // (camel-rabbitmq: naglowek CamelRabbitmqRequeue=true => basicReject(requeue=true)).
            exchange.getMessage().setHeader(REQUEUE_HEADER, true);
        }
    }

    private void open(Route route, String why) {
        state = State.OPEN;
        failures.clear();
        LOG.warn("Circuit breaker [{}]: OPEN ({}) - wstrzymuje konsumenta na {} ms", route.getId(), why, halfOpenAfterMs);
        executor.execute(() -> {
            try {
                stopConsumer(route.getConsumer());
            } catch (Exception e) {
                LOG.warn("Circuit breaker [{}]: nie udalo sie zatrzymac konsumenta", route.getId(), e);
            }
        });
        executor.schedule(() -> halfOpen(route), halfOpenAfterMs, TimeUnit.MILLISECONDS);
    }

    private void halfOpen(Route route) {
        synchronized (this) {
            state = State.HALF_OPEN;
        }
        LOG.info("Circuit breaker [{}]: HALF_OPEN - wznawiam konsumenta na probe", route.getId());
        try {
            startConsumer(route.getConsumer());
        } catch (Exception e) {
            // np. broker niedostepny - sprobujemy ponownie po kolejnym okresie
            LOG.warn("Circuit breaker [{}]: nie udalo sie wznowic konsumenta, ponowie probe", route.getId(), e);
            executor.schedule(() -> halfOpen(route), halfOpenAfterMs, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized State getState() {
        return state;
    }
}
