package pl.isp.allegro.common.camel;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.http.base.HttpOperationFailedException;

import java.util.function.IntPredicate;

/**
 * Wspolna polityka odpornosci dla tras wolajacych API Allegro (kolektor i aktualizator):
 * Retry z backoffem zalezne od kodu HTTP oraz Circuit Breaker na konsumencie kolejki.
 */
public final class HttpResilience {

    private HttpResilience() {
    }

    /**
     * Retry:
     *  - 429 Too Many Requests: limit API - dluzsza, stala przerwa miedzy probami,
     *  - 5xx: blad chwilowy serwera - wykladniczy backoff,
     *  - bledy sieciowe (IOException: timeout, connection refused): wykladniczy backoff,
     *  - pozostale 4xx: blad trwaly, ponawianie nie ma sensu.
     * Po wyczerpaniu prob wyjatek NIE jest oznaczany jako obsluzony - konsument RabbitMQ odrzuca
     * komunikat (nack, requeue=false), a broker przenosi go do kolejki DLQ.
     */
    public static void retryPolicy(RouteBuilder rb, String maxRetries, String delayMs, String delay429Ms) {
        rb.onException(HttpOperationFailedException.class).onWhen(status(code -> code == 429))
                .maximumRedeliveries(maxRetries).redeliveryDelay(delay429Ms)
                .retryAttemptedLogLevel(LoggingLevel.WARN).logExhaustedMessageHistory(false).logStackTrace(false);

        rb.onException(HttpOperationFailedException.class).onWhen(status(code -> code >= 500))
                .maximumRedeliveries(maxRetries).redeliveryDelay(delayMs)
                .useExponentialBackOff().backOffMultiplier(2).maximumRedeliveryDelay(10000)
                .retryAttemptedLogLevel(LoggingLevel.WARN).logExhaustedMessageHistory(false).logStackTrace(false);

        rb.onException(HttpOperationFailedException.class)
                .maximumRedeliveries(0).logExhaustedMessageHistory(false).logStackTrace(false);

        rb.onException(java.io.IOException.class)
                .maximumRedeliveries(maxRetries).redeliveryDelay(delayMs)
                .useExponentialBackOff().backOffMultiplier(2).maximumRedeliveryDelay(10000)
                .retryAttemptedLogLevel(LoggingLevel.WARN).logExhaustedMessageHistory(false).logStackTrace(false);
    }

    /** Circuit Breaker na konsumencie kolejki - patrz {@link QueueCircuitBreaker}. */
    public static QueueCircuitBreaker circuitBreaker(int failureThreshold, long failureWindowMs, long halfOpenAfterMs) {
        return new QueueCircuitBreaker(failureThreshold, failureWindowMs, halfOpenAfterMs, HttpResilience::isServiceFailure);
    }

    /** Awaria uslugi (5xx, 429, siec, broker) w odroznieniu od wadliwego komunikatu (walidacja, 4xx). */
    static boolean isServiceFailure(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof IllegalArgumentException) {
                return false;
            }
            if (t instanceof HttpOperationFailedException) {
                int code = ((HttpOperationFailedException) t).getStatusCode();
                return code == 429 || code >= 500;
            }
        }
        return true;
    }

    private static Predicate status(IntPredicate test) {
        return exchange -> {
            Throwable t = exchange.getException();
            if (t == null) {
                t = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
            }
            while (t != null && !(t instanceof HttpOperationFailedException)) {
                t = t.getCause();
            }
            return t != null && test.test(((HttpOperationFailedException) t).getStatusCode());
        };
    }
}
