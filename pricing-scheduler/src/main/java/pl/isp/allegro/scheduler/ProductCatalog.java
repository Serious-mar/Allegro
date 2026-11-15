package pl.isp.allegro.scheduler;

import pl.isp.allegro.common.Json;
import pl.isp.allegro.common.model.ProductTask;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Lista produktow do monitorowania. Czytana przy kazdym uruchomieniu harmonogramu z pliku
 * (domyslnie ${karaf.etc}/products.json), wiec mozna ja zmieniac bez restartu bundla.
 * Gdy pliku nie ma, uzywana jest lista wbudowana w bundle.
 */
public class ProductCatalog {

    private final String file;

    public ProductCatalog(String file) {
        this.file = file;
    }

    public List<ProductTask> load() throws Exception {
        Path path = resolve();
        if (path != null && Files.isReadable(path)) {
            return Arrays.asList(Json.MAPPER.readValue(path.toFile(), ProductTask[].class));
        }
        try (InputStream in = ProductCatalog.class.getResourceAsStream("/products.json")) {
            return Arrays.asList(Json.MAPPER.readValue(in, ProductTask[].class));
        }
    }

    private Path resolve() {
        if (file != null && !file.trim().isEmpty()) {
            return Paths.get(file.trim());
        }
        String etc = System.getProperty("karaf.etc");
        return etc == null ? null : Paths.get(etc, "products.json");
    }
}
