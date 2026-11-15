package pl.isp.allegro.mock;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import pl.isp.allegro.common.Json;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Symulowany rynek: dla kazdego produktu kilku konkurentow, ktorych ceny wykonuja bladzenie losowe
 * wokol ceny bazowej. Od czasu do czasu ktorys konkurent robi "dumping" (ok. 20% ponizej bazy),
 * co pozwala zaobserwowac alerty o zejsciu ponizej naszej minimalnej marzy.
 */
public class MarketSimulator {

    private static final String[] SELLERS = {"TechOutlet", "MegaSklep24", "ElektroHurt", "GadzetMania", "TanioNet", "ProAudioPL"};
    private static final Map<String, Double> BASE_PRICES = Map.of(
            "P-1001", 230.0, "P-1002", 300.0, "P-1003", 125.0,
            "P-1004", 92.0, "P-1005", 165.0, "P-1006", 140.0);

    private final String ourSeller;
    private final Random random = new Random();
    private final Map<String, Market> markets = new LinkedHashMap<>();

    public MarketSimulator(String ourSeller) {
        this.ourSeller = ourSeller;
    }

    /** Odpowiedz w ksztalcie GET /offers/listing z REST API Allegro. */
    public synchronized String listing(String productId) {
        Market m = market(productId);
        m.tick();
        ArrayNode regular = Json.MAPPER.createArrayNode();
        regular.add(item(m.ourOfferId, m.name, ourSeller, m.ourPrice));
        for (Competitor c : m.competitors) {
            regular.add(item(c.offerId, m.name, c.seller, c.price));
        }
        ObjectNode root = Json.MAPPER.createObjectNode();
        ObjectNode items = root.putObject("items");
        items.putArray("promoted");
        items.set("regular", regular);
        root.putObject("searchMeta").put("availableCount", regular.size()).put("totalCount", regular.size());
        return root.toString();
    }

    /** PATCH /sale/product-offers/{offerId} - zmiana ceny naszej oferty. Zwraca null, gdy oferty nie ma. */
    public synchronized String updatePrice(String offerId, BigDecimal newPrice) {
        for (Market m : markets.values()) {
            if (m.ourOfferId.equals(offerId)) {
                m.ourPrice = newPrice.doubleValue();
                return item(m.ourOfferId, m.name, ourSeller, m.ourPrice).toString();
            }
        }
        return null;
    }

    private Market market(String productId) {
        return markets.computeIfAbsent(productId, id -> new Market(id, BASE_PRICES.getOrDefault(id, 100.0)));
    }

    private ObjectNode item(String id, String name, String seller, double price) {
        ObjectNode item = Json.MAPPER.createObjectNode();
        item.put("id", id);
        item.put("name", name);
        item.putObject("seller").put("id", String.valueOf(Math.abs(seller.hashCode()))).put("login", seller);
        item.putObject("sellingMode").put("format", "BUY_NOW").putObject("price")
                .put("amount", BigDecimal.valueOf(price).setScale(2, RoundingMode.HALF_UP).toPlainString())
                .put("currency", "PLN");
        return item;
    }

    private final class Market {
        final String name;
        final String ourOfferId;
        final double base;
        double ourPrice;
        final List<Competitor> competitors = new ArrayList<>();

        Market(String productId, double base) {
            this.name = "Produkt " + productId;
            this.ourOfferId = "OUR-" + productId;
            this.base = base;
            this.ourPrice = base * 1.10;
            int count = 4 + random.nextInt(3);
            for (int i = 0; i < count; i++) {
                Competitor c = new Competitor();
                c.offerId = "C" + i + "-" + productId;
                c.seller = SELLERS[i];
                c.anchor = base * (0.97 + random.nextDouble() * 0.28);
                c.price = c.anchor;
                competitors.add(c);
            }
        }

        void tick() {
            for (Competitor c : competitors) {
                if (c.dumpTicks > 0) {
                    if (--c.dumpTicks == 0) {
                        c.price = c.anchor;
                    }
                    continue;
                }
                if (random.nextDouble() < 0.008) {
                    c.dumpTicks = 2 + random.nextInt(3);
                    c.price = base * (0.78 + random.nextDouble() * 0.05);
                    continue;
                }
                // bladzenie losowe z powrotem do sredniej
                c.price += (c.anchor - c.price) * 0.15 + base * 0.02 * random.nextGaussian();
                c.price = Math.max(c.price, base * 0.9);
            }
        }
    }

    private static final class Competitor {
        String offerId;
        String seller;
        double anchor;
        double price;
        int dumpTicks;
    }
}
