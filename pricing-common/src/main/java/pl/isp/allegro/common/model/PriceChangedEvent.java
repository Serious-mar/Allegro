package pl.isp.allegro.common.model;

import java.math.BigDecimal;

/** Potwierdzenie wykonanej zmiany ceny - zdarzenie na topicu Kafki price.changes. */
public class PriceChangedEvent {
    public String commandId;
    public String productId;
    public String offerId;
    public BigDecimal oldPrice;
    public BigDecimal newPrice;
    public String reason;
    public String changedAt;
}
