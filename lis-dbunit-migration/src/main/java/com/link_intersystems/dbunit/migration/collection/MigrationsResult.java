package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MigrationsResult extends AbstractMap<DataSetResource, DataSetResource> {

    private Map<DataSetResource, DataSetResource> migratedDataSetResources;

    MigrationsResult(Map<DataSetResource, DataSetResource> migratedDataSetResources) {
        this.migratedDataSetResources = requireNonNull(migratedDataSetResources);
    }

    @Override
    public Set<Entry<DataSetResource, DataSetResource>> entrySet() {
        return Collections.unmodifiableSet(migratedDataSetResources.entrySet());
    }
}
