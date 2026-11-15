package pl.isp.allegro.common.model;

import java.math.BigDecimal;

/** Alert "konkurencja ponizej minimalnej marzy" - komunikat MQTT alerts/price/{productId}, QoS 1. */
public class PriceAlert {
    public String alertId;
    public String productId;
    public String name;
    public BigDecimal ourPrice;
    public BigDecimal minAllowedPrice;
    public BigDecimal competitorPrice;
    public String competitorSeller;
    public String message;
    public String at;
}
