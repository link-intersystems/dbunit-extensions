package com.link_intersystems.dbunit.migration;

import java.io.File;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BasepathTargetPathSupplier implements TargetPathSupplier {

    private Path basepath;

    public BasepathTargetPathSupplier() {
        this(new File(System.getProperty("user.dir")));
    }

    public BasepathTargetPathSupplier(File basedir) {
        this(basedir.toPath());
    }

    public BasepathTargetPathSupplier(Path basepath) {
        this.basepath = requireNonNull(basepath);
    }

    @Override
    public Path getTarget(Path path) {
        return basepath.resolve(path);
    }
}
