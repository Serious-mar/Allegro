package pl.isp.allegro.common.model;

public enum Decision {
    /** Nic nie robimy. */
    HOLD,
    /** Zmiana ceny miesci sie w marzy -> komenda na RabbitMQ. */
    CHANGE_PRICE,
    /** Konkurencja ponizej naszej minimalnej ceny -> alert MQTT. */
    ALERT
}
