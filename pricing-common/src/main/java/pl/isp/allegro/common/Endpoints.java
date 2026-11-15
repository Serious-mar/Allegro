package pl.isp.allegro.common;

/**
 * Jedno miejsce z definicjami endpointow brokerow. Producent i konsument kolejki RabbitMQ musza
 * deklarowac ja z identycznymi argumentami (DLX), dlatego URI buduje wspolny kod.
 * Wartosci {{...}} pochodza z ConfigAdmin (PID pl.isp.allegro, plik etc/pl.isp.allegro.cfg).
 */
public final class Endpoints {

    /** Kolejka zadan "sprawdz produkt" (harmonogram -> kolektor). */
    public static final String QUEUE_TASKS = "offer.check.tasks";
    /** Kolejka komend zmiany ceny (analizator -> aktualizator). */
    public static final String QUEUE_COMMANDS = "price.change.commands";

    /** Surowe wyniki z API Allegro. */
    public static final String TOPIC_OFFERS = "market.offers";
    /** Statystyki cenowe i decyzje silnika regul. */
    public static final String TOPIC_STATS = "market.stats";
    /** Historia faktycznie wykonanych zmian cen. */
    public static final String TOPIC_PRICE_CHANGES = "price.changes";
    /** Dead letter topic dla zdarzen, ktorych analizator nie umie przetworzyc. */
    public static final String TOPIC_OFFERS_DLT = "market.offers.dlt";

    public static final String MQTT_ALERT_TOPIC_PREFIX = "alerts/price/";
    public static final String MQTT_DIGEST_TOPIC = "alerts/summary";

    private static final String RABBIT_COMMON =
            "rabbitmq:pricing?hostname={{rabbitmq.host}}&portNumber={{rabbitmq.port}}"
            + "&username={{rabbitmq.username}}&password={{rabbitmq.password}}"
            + "&exchangeType=direct&durable=true&autoDelete=false"
            // odpornosc na awarie brokera: klient AMQP sam odtwarza polaczenie i topologie
            + "&automaticRecoveryEnabled=true&topologyRecoveryEnabled=true&networkRecoveryInterval=5000"
            // DLQ po stronie brokera: odrzucony (nack, requeue=false) komunikat trafia do <kolejka>.dlq
            + "&deadLetterExchange=pricing.dlx&deadLetterExchangeType=direct";

    private Endpoints() {
    }

    public static String rabbitProducer(String queue) {
        return RABBIT_COMMON + "&queue=" + queue + "&routingKey=" + queue
                + "&deadLetterQueue=" + queue + ".dlq&deadLetterRoutingKey=" + queue;
    }

    public static String rabbitConsumer(String queue, int consumers, int prefetch) {
        // autoAck=false: ack dopiero po poprawnym przejsciu calej trasy (at-least-once)
        return rabbitProducer(queue) + "&autoAck=false&concurrentConsumers=" + consumers
                + "&prefetchEnabled=true&prefetchCount=" + prefetch + "&prefetchSize=0&prefetchGlobal=false";
    }

    public static String kafkaProducer(String topic) {
        // maxBlockMs: przy niedostepnej Kafce producent zglasza blad po 10 s zamiast wisiec minute
        return "kafka:" + topic + "?brokers={{kafka.brokers}}&requestRequiredAcks=all&retries=3&maxBlockMs=10000";
    }

    public static String kafkaConsumer(String topic, String groupId) {
        return "kafka:" + topic + "?brokers={{kafka.brokers}}&groupId=" + groupId
                + "&autoOffsetReset=earliest&autoCommitIntervalMs=1000";
    }

    public static String mqttProducer(String topic, int qos, boolean retained, String clientId) {
        return "paho:" + topic + "?brokerUrl=tcp://{{mqtt.host}}:{{mqtt.port}}&qos=" + qos
                + "&retained=" + retained + "&clientId=" + clientId + "&automaticReconnect=true";
    }

    public static String mqttConsumer(String topicFilter, int qos, String clientId) {
        // cleanSession=false + staly clientId: broker kolejkuje komunikaty QoS>=1 na czas nieobecnosci subskrybenta
        return "paho:" + topicFilter + "?brokerUrl=tcp://{{mqtt.host}}:{{mqtt.port}}&qos=" + qos
                + "&clientId=" + clientId + "&cleanSession=false&automaticReconnect=true";
    }
}
