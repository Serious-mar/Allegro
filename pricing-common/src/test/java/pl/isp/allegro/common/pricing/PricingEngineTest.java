package pl.isp.allegro.common.pricing;

import org.junit.jupiter.api.Test;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.model.Decision;
import pl.isp.allegro.common.model.MarketStats;
import pl.isp.allegro.common.model.Offer;
import pl.isp.allegro.common.model.ProductTask;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PricingEngineTest {

    private final PricingEngine engine = new PricingEngine(new PricingRules());

    /** koszt 100, marza 10% => cena minimalna 110.00 */
    private static ProductTask task() {
        ProductTask t = new ProductTask();
        t.productId = "P1";
        t.ourOfferId = "OUR";
        t.purchaseCost = new BigDecimal("100.00");
        t.minMarginPct = new BigDecimal("10");
        return t;
    }

    private MarketStats analyze(String ourPrice, String... competitors) {
        List<Offer> offers = new ArrayList<>();
        offers.add(new Offer("OUR", "my", new BigDecimal(ourPrice)));
        for (int i = 0; i < competitors.length; i++) {
            offers.add(new Offer("C" + i, "konkurent" + i, new BigDecimal(competitors[i])));
        }
        return engine.analyze(task(), offers, "e1", "now");
    }

    private static void assertPrice(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual), "oczekiwano " + expected + ", jest " + actual);
    }

    @Test
    void statystyki() {
        MarketStats s = analyze("130.00", "120.00", "150.00", "140.00");
        assertEquals(4, s.offerCount);
        assertPrice("120.00", s.min);
        assertPrice("150.00", s.max);
        assertPrice("135.00", s.median);
        assertEquals(2, s.ourPosition);
        assertPrice("110.00", s.minAllowedPrice);
        assertEquals("konkurent0", s.competitorMinSeller);
    }

    @Test
    void medianaNieparzystejLiczbyOfert() {
        assertPrice("130.00", analyze("130.00", "120.00", "150.00").median);
    }

    @Test
    void tanszyKonkurentIMarzaPozwala_zmianaCeny() {
        MarketStats s = analyze("130.00", "120.00", "150.00");
        assertEquals(Decision.CHANGE_PRICE, s.decision);
        assertPrice("119.99", s.targetPrice);
    }

    @Test
    void tanszyKonkurentTuzNadProgiem_schodzimyDoCenyMinimalnej() {
        MarketStats s = analyze("130.00", "110.00");
        assertEquals(Decision.CHANGE_PRICE, s.decision);
        assertPrice("110.00", s.targetPrice);
    }

    @Test
    void konkurentPonizejMinimalnejMarzy_alert() {
        MarketStats s = analyze("130.00", "109.99", "150.00");
        assertEquals(Decision.ALERT, s.decision);
        assertNull(s.targetPrice);
    }

    @Test
    void jestesmyNajtansiZDuzymZapasem_podniesienieCeny() {
        MarketStats s = analyze("115.00", "140.00");
        assertEquals(Decision.CHANGE_PRICE, s.decision);
        assertPrice("139.99", s.targetPrice);
        assertEquals(1, s.ourPosition);
    }

    @Test
    void podnoszenieWylaczone_hold() {
        PricingEngine noRaise = new PricingEngine(new PricingRules("0.01", false, "0.5"));
        List<Offer> offers = List.of(new Offer("OUR", "my", new BigDecimal("115.00")),
                new Offer("C", "k", new BigDecimal("140.00")));
        assertEquals(Decision.HOLD, noRaise.analyze(task(), offers, "e", "now").decision);
    }

    @Test
    void cenaJuzOptymalna_hold() {
        assertEquals(Decision.HOLD, analyze("119.99", "120.00").decision);
    }

    @Test
    void brakKonkurencji_hold() {
        assertEquals(Decision.HOLD, analyze("130.00").decision);
    }

    @Test
    void cenaMinimalnaZaokraglanaWGore() {
        assertPrice("36.93", PricingEngine.minAllowedPrice(new BigDecimal("33.33"), new BigDecimal("10.8")));
    }

    @Test
    void parsowanieListinguAllegro() {
        String json = "{\"items\":{\"promoted\":[{\"id\":\"1\",\"seller\":{\"id\":\"s1\",\"login\":\"A\"},"
                + "\"sellingMode\":{\"price\":{\"amount\":\"10.50\",\"currency\":\"PLN\"}}}],"
                + "\"regular\":[{\"id\":\"2\",\"seller\":{\"id\":\"s2\"},\"sellingMode\":{\"price\":{\"amount\":\"9.99\"}}}]}}";
        List<Offer> offers = AllegroListingParser.parse(Json.tree(json));
        assertEquals(2, offers.size());
        assertEquals("A", offers.get(0).seller);
        assertEquals("s2", offers.get(1).seller);
        assertPrice("9.99", offers.get(1).price);
        assertThrows(IllegalArgumentException.class, () -> AllegroListingParser.parse(Json.tree("{\"errors\":[]}")));
    }
}
