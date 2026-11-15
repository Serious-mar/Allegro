package pl.isp.allegro.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;

/** Wspolna (de)serializacja JSON dla wszystkich komunikatow przesylanych przez brokery. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Json() {
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Nie mozna zserializowac " + value, e);
        }
    }

    public static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Niepoprawny JSON dla " + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    public static JsonNode tree(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("Niepoprawny JSON: " + e.getMessage(), e);
        }
    }

    /** Odczytuje tresc komunikatu Camela (byte[] z RabbitMQ/MQTT lub String z Kafki) jako obiekt. */
    public static <T> T body(Exchange exchange, Class<T> type) {
        return read(exchange.getIn().getBody(String.class), type);
    }
}
