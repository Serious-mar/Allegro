package pl.isp.allegro.common.pricing;

import java.math.BigDecimal;

/** Parametry silnika regul. */
public class PricingRules {
    /** O ile schodzimy ponizej najtanszego konkurenta. */
    public BigDecimal undercutStep = new BigDecimal("0.01");
    /** Czy podnosic cene, gdy jestesmy najtansi z duzym zapasem (odzyskiwanie marzy). */
    public boolean raiseEnabled = true;
    /** Minimalna roznica (w procentach naszej ceny), przy ktorej oplaca sie wysylac komende. */
    public BigDecimal minChangePct = new BigDecimal("0.5");

    public PricingRules() {
    }

    public PricingRules(String undercutStep, boolean raiseEnabled, String minChangePct) {
        this.undercutStep = new BigDecimal(undercutStep);
        this.raiseEnabled = raiseEnabled;
        this.minChangePct = new BigDecimal(minChangePct);
    }
}
