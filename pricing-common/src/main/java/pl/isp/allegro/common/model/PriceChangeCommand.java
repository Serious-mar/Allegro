package pl.isp.allegro.common.model;

import java.math.BigDecimal;

/** Komenda zmiany ceny - komunikat na kolejce RabbitMQ price.change.commands. */
public class PriceChangeCommand {
    public String commandId;
    public String productId;
    public String offerId;
    public BigDecimal oldPrice;
    public BigDecimal newPrice;
    public String reason;
    public String createdAt;
}
