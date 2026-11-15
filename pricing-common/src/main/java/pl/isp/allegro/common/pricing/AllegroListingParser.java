package pl.isp.allegro.common.pricing;

import com.fasterxml.jackson.databind.JsonNode;
import pl.isp.allegro.common.model.Offer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/** Wyciaga oferty z odpowiedzi GET /offers/listing (items.promoted + items.regular). */
public final class AllegroListingParser {

    private AllegroListingParser() {
    }

    public static List<Offer> parse(JsonNode listing) {
        if (listing == null || !listing.path("items").isObject()) {
            throw new IllegalArgumentException("Odpowiedz listingu nie zawiera pola 'items'");
        }
        List<Offer> offers = new ArrayList<>();
        for (String section : new String[] {"promoted", "regular"}) {
            for (JsonNode item : listing.path("items").path(section)) {
                JsonNode amount = item.path("sellingMode").path("price").path("amount");
                if (amount.isMissingNode() || amount.isNull()) {
                    continue;
                }
                offers.add(new Offer(item.path("id").asText(),
                        item.path("seller").path("login").asText(item.path("seller").path("id").asText()),
                        new BigDecimal(amount.asText())));
            }
        }
        return offers;
    }
}
