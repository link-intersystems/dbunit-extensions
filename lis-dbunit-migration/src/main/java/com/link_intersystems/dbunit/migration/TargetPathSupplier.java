package com.link_intersystems.dbunit.migration;

import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TargetPathSupplier {
    Path getTarget(Path path);
}
