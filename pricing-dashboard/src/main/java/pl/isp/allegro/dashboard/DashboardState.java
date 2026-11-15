package pl.isp.allegro.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import pl.isp.allegro.common.Json;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Stan dashboardu w pamieci - odtwarzany po restarcie z historii w Kafce (seekTo=beginning). */
public class DashboardState {

    private static final int MAX_HISTORY = 400;
    private static final int MAX_EVENTS = 100;

    private final Map<String, Deque<JsonNode>> history = new TreeMap<>();
    private final Deque<JsonNode> changes = new ArrayDeque<>();
    private final Deque<JsonNode> alerts = new ArrayDeque<>();
    private JsonNode digest;
    private long statsCount;
    private long changeCount;
    private long alertCount;

    public synchronized void onStats(String json) {
        JsonNode stats = Json.tree(json);
        push(history.computeIfAbsent(stats.path("productId").asText(), k -> new ArrayDeque<>()), stats, MAX_HISTORY);
        statsCount++;
    }

    public synchronized void onPriceChange(String json) {
        push(changes, Json.tree(json), MAX_EVENTS);
        changeCount++;
    }

    public synchronized void onAlert(String json) {
        push(alerts, Json.tree(json), MAX_EVENTS);
        alertCount++;
    }

    public synchronized void onDigest(String json) {
        digest = Json.tree(json);
    }

    public synchronized String snapshot() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("statsCount", statsCount);
        root.put("changeCount", changeCount);
        root.put("alertCount", alertCount);
        root.put("products", history);
        root.put("changes", changes);
        root.put("alerts", alerts);
        root.put("digest", digest);
        return Json.write(root);
    }

    private static void push(Deque<JsonNode> deque, JsonNode item, int max) {
        deque.addLast(item);
        while (deque.size() > max) {
            deque.removeFirst();
        }
    }
}
