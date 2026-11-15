package pl.isp.allegro.common.model;

import com.fasterxml.jackson.databind.JsonNode;

/** Surowy wynik odpytania API Allegro - zdarzenie na topicu Kafki market.offers (klucz = productId). */
public class MarketOffersEvent {
    /** Rowny taskId - pozwala wykryc duplikaty (Idempotent Consumer). */
    public String eventId;
    public ProductTask task;
    public String collectedAt;
    /** Nieprzetworzona odpowiedz GET /offers/listing. */
    public JsonNode listing;
}
