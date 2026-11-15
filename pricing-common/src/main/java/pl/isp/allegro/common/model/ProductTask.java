package pl.isp.allegro.common.model;

import java.math.BigDecimal;

/** Zadanie "sprawdz rynek dla produktu" - komunikat na kolejce RabbitMQ offer.check.tasks. */
public class ProductTask {
    public String taskId;
    public String productId;
    public String name;
    /** Identyfikator naszej oferty na Allegro. */
    public String ourOfferId;
    /** Koszt zakupu - podstawa do wyliczenia minimalnej ceny. */
    public BigDecimal purchaseCost;
    /** Minimalna marza w procentach kosztu zakupu. */
    public BigDecimal minMarginPct;
    public String scheduledAt;
}
