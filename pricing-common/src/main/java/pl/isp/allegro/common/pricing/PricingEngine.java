package pl.isp.allegro.common.pricing;

import pl.isp.allegro.common.model.Decision;
import pl.isp.allegro.common.model.MarketOffersEvent;
import pl.isp.allegro.common.model.MarketStats;
import pl.isp.allegro.common.model.Offer;
import pl.isp.allegro.common.model.ProductTask;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Statystyki cenowe (min, mediana, pozycja naszej oferty) oraz silnik regul.
 * Czysta logika bez zaleznosci od Camela - latwa do testowania jednostkowego.
 */
public class PricingEngine {

    private static final BigDecimal HUNDRED = new BigDecimal("100");
    private static final BigDecimal TWO = new BigDecimal("2");

    private final PricingRules rules;

    public PricingEngine(PricingRules rules) {
        this.rules = rules;
    }

    public MarketStats analyze(MarketOffersEvent event) {
        ProductTask task = event.task;
        if (task == null || task.ourOfferId == null || task.purchaseCost == null || task.minMarginPct == null) {
            throw new IllegalArgumentException("Zdarzenie bez kompletnych danych zadania (eventId=" + event.eventId + ")");
        }
        return analyze(task, AllegroListingParser.parse(event.listing), event.eventId, event.collectedAt);
    }

    public MarketStats analyze(ProductTask task, List<Offer> offers, String eventId, String at) {
        MarketStats s = new MarketStats();
        s.eventId = eventId;
        s.productId = task.productId;
        s.name = task.name;
        s.ourOfferId = task.ourOfferId;
        s.at = at;
        s.minAllowedPrice = minAllowedPrice(task.purchaseCost, task.minMarginPct);

        List<Offer> sorted = new ArrayList<>(offers);
        sorted.sort(Comparator.comparing(o -> o.price));
        s.offerCount = sorted.size();
        s.prices = new ArrayList<>();
        Offer cheapestCompetitor = null;
        for (int i = 0; i < sorted.size(); i++) {
            Offer o = sorted.get(i);
            s.prices.add(o.price);
            if (task.ourOfferId.equals(o.offerId)) {
                s.ourPrice = o.price;
                // pozycja = 1 + liczba ofert scisle tanszych (remis nie pogarsza pozycji)
                s.ourPosition = 1 + (int) sorted.stream().filter(x -> x.price.compareTo(o.price) < 0).count();
            } else if (cheapestCompetitor == null) {
                cheapestCompetitor = o;
            }
        }
        if (!sorted.isEmpty()) {
            s.min = sorted.get(0).price;
            s.max = sorted.get(sorted.size() - 1).price;
            s.median = median(s.prices);
        }
        if (cheapestCompetitor != null) {
            s.competitorMin = cheapestCompetitor.price;
            s.competitorMinSeller = cheapestCompetitor.seller;
        }
        decide(s);
        return s;
    }

    /** Minimalna dopuszczalna cena = koszt * (1 + marza%), zaokraglona w gore do grosza. */
    public static BigDecimal minAllowedPrice(BigDecimal cost, BigDecimal minMarginPct) {
        return cost.multiply(BigDecimal.ONE.add(minMarginPct.divide(HUNDRED, 6, RoundingMode.HALF_UP)))
                .setScale(2, RoundingMode.CEILING);
    }

    static BigDecimal median(List<BigDecimal> sortedPrices) {
        int n = sortedPrices.size();
        if (n % 2 == 1) {
            return sortedPrices.get(n / 2);
        }
        return sortedPrices.get(n / 2 - 1).add(sortedPrices.get(n / 2)).divide(TWO, 2, RoundingMode.HALF_UP);
    }

    private void decide(MarketStats s) {
        if (s.ourPrice == null) {
            hold(s, "Naszej oferty nie ma na liscie");
            return;
        }
        if (s.competitorMin == null) {
            hold(s, "Brak ofert konkurencji");
            return;
        }
        if (s.competitorMin.compareTo(s.minAllowedPrice) < 0) {
            // Konkurencja zeszla ponizej naszej minimalnej marzy - nie scigamy sie, alarmujemy czlowieka.
            s.decision = Decision.ALERT;
            s.reason = "Konkurent " + s.competitorMinSeller + " (" + s.competitorMin
                    + ") ponizej naszej ceny minimalnej " + s.minAllowedPrice;
            return;
        }
        // Cena docelowa: tuz pod najtanszym konkurentem, ale nigdy ponizej minimalnej marzy.
        BigDecimal target = s.competitorMin.subtract(rules.undercutStep).max(s.minAllowedPrice)
                .setScale(2, RoundingMode.HALF_UP);
        int cmp = target.compareTo(s.ourPrice);
        if (cmp == 0) {
            hold(s, "Cena optymalna");
        } else if (cmp < 0) {
            if (s.competitorMin.compareTo(s.ourPrice) < 0) {
                change(s, target, "Tanszy konkurent " + s.competitorMinSeller + " (" + s.competitorMin + ")");
            } else {
                // remis z konkurentem - schodzimy o krok tylko jesli marza pozwala (target < ourPrice to gwarantuje)
                change(s, target, "Remis cenowy z " + s.competitorMinSeller);
            }
        } else if (rules.raiseEnabled && significant(s.ourPrice, target)) {
            change(s, target, "Jestesmy najtansi - podniesienie ceny pod konkurenta " + s.competitorMinSeller
                    + " (" + s.competitorMin + ")");
        } else {
            hold(s, "Jestesmy najtansi");
        }
    }

    private boolean significant(BigDecimal ourPrice, BigDecimal target) {
        BigDecimal diffPct = target.subtract(ourPrice).abs().multiply(HUNDRED).divide(ourPrice, 4, RoundingMode.HALF_UP);
        return diffPct.compareTo(rules.minChangePct) >= 0;
    }

    private static void hold(MarketStats s, String reason) {
        s.decision = Decision.HOLD;
        s.reason = reason;
    }

    private static void change(MarketStats s, BigDecimal target, String reason) {
        s.decision = Decision.CHANGE_PRICE;
        s.targetPrice = target;
        s.reason = reason;
    }
}
