package pl.isp.allegro.common.model;

import java.math.BigDecimal;

public class Offer {
    public String offerId;
    public String seller;
    public BigDecimal price;

    public Offer() {
    }

    public Offer(String offerId, String seller, BigDecimal price) {
        this.offerId = offerId;
        this.seller = seller;
        this.price = price;
    }
}
