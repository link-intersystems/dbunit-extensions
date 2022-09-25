package com.link_intersystems.dbunit.migration.flyway;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface PlaceholdersSource {

    public static PlaceholdersSource fromMap(Consumer<Map<String, String>> initializer) {
        Map<String, String> placeholders = new HashMap<>();
        initializer.accept(placeholders);
        return () -> placeholders;
    }

    public Map<String, String> getPlaceholders();
}
