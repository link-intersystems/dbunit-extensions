package com.link_intersystems.dbunit.migration.flyway;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.link_intersystems.dbunit.migration.flyway.PlaceholdersSource.fromMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class PlaceholdersSourceChainTest {

    @Test
    void getPlaceholders() {
        PlaceholdersSource basicPlaceholders = fromMap(placeholders -> {
            placeholders.put("a", "value a");
            placeholders.put("b", "value b");
        });

        PlaceholdersSource overridePlaceholders = fromMap(placeholders -> placeholders.put("b", "value b override"));
        PlaceholdersSource additionalPlaceholders = fromMap(placeholders -> placeholders.put("c", "value c"));

        PlaceholdersSourceChain placeholdersSourceChain = new PlaceholdersSourceChain(basicPlaceholders, overridePlaceholders, additionalPlaceholders);

        Map<String, String> placeholders = placeholdersSourceChain.getPlaceholders();
        assertEquals("value a", placeholders.get("a"));
        assertEquals("value b override", placeholders.get("b"));
        assertEquals("value c", placeholders.get("c"));
    }

}