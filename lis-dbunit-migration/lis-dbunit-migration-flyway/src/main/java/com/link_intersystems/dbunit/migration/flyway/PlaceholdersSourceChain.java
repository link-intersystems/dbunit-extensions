package com.link_intersystems.dbunit.migration.flyway;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class PlaceholdersSourceChain implements PlaceholdersSource {

    private List<PlaceholdersSource> placeholdersSources;

    public PlaceholdersSourceChain(PlaceholdersSource... placeholdersSources) {
        this(Arrays.asList(placeholdersSources));
    }

    public PlaceholdersSourceChain(List<PlaceholdersSource> placeholdersSources) {
        this.placeholdersSources = requireNonNull(placeholdersSources);
    }

    @Override
    public Map<String, String> getPlaceholders() {
        Map<String, String> placeholders = new HashMap<>();

        for (PlaceholdersSource placeholdersSource : placeholdersSources) {
            placeholders.putAll(placeholdersSource.getPlaceholders());
        }

        return placeholders;
    }

}
