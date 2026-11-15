package pl.isp.allegro.scheduler;

import org.apache.camel.builder.RouteBuilder;
import pl.isp.allegro.common.Endpoints;
import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.model.ProductTask;

import java.time.Instant;
import java.util.UUID;

/**
 * Harmonogram: Quartz (cron) -> lista produktow -> SPLITTER -> pojedyncze zadania na kolejke RabbitMQ.
 */
public class SchedulerRoutes extends RouteBuilder {

    private ProductCatalog catalog;
    private String triggerUri = "quartz://pricing/scan?cron={{scheduler.cron}}";
    private String tasksUri = Endpoints.rabbitProducer(Endpoints.QUEUE_TASKS);

    @Override
    public void configure() {
        // Awaria RabbitMQ: nie ponawiamy w nieskonczonosc - za chwile i tak bedzie kolejny cykl.
        onException(Exception.class).handled(true)
                .log("Harmonogram: nie udalo sie zakolejkowac zadania: ${exception.message}");

        from(triggerUri).routeId("scheduler")
                .bean(catalog, "load")
                .log("Harmonogram: ${body.size()} produktow do sprawdzenia")
                // EIP Splitter: jedna lista -> N niezaleznych zadan
                .split(body())
                    .process(exchange -> {
                        ProductTask task = exchange.getIn().getBody(ProductTask.class);
                        task.taskId = UUID.randomUUID().toString();
                        task.scheduledAt = Instant.now().toString();
                        exchange.getIn().setBody(Json.write(task));
                    })
                    // komunikat trwaly: przezyje restart brokera (kolejka durable + delivery mode 2)
                    .setHeader("CamelRabbitmqDeliveryMode", constant(2))
                    .setHeader("CamelRabbitmqContentType", constant("application/json"))
                    .to(tasksUri)
                .end();
    }

    public void setCatalog(ProductCatalog catalog) {
        this.catalog = catalog;
    }

    public void setTriggerUri(String triggerUri) {
        this.triggerUri = triggerUri;
    }

    public void setTasksUri(String tasksUri) {
        this.tasksUri = tasksUri;
    }
}
