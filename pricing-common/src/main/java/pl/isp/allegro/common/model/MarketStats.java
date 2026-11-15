package pl.isp.allegro.common.model;

import java.math.BigDecimal;
import java.util.List;

/** Wynik analizy jednego zdarzenia: statystyki rynku + decyzja silnika regul (topic market.stats). */
public class MarketStats {
    public String eventId;
    public String productId;
    public String name;
    public String ourOfferId;
    public String at;

    public int offerCount;
    public BigDecimal min;
    public BigDecimal median;
    public BigDecimal max;
    /** Posortowane rosnaco ceny wszystkich ofert (rozklad cen na rynku). */
    public List<BigDecimal> prices;

    public BigDecimal ourPrice;
    /** Pozycja naszej oferty w rankingu cenowym (1 = najtansza); 0 gdy naszej oferty nie ma na liscie. */
    public int ourPosition;
    public BigDecimal competitorMin;
    public String competitorMinSeller;
    /** Najnizsza cena, przy ktorej zachowujemy minimalna marze. */
    public BigDecimal minAllowedPrice;

    public Decision decision;
    public BigDecimal targetPrice;
    public String reason;
}
