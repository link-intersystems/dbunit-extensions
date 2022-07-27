package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import org.dbunit.dataset.DataSetException;

import java.io.File;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BasepathTargetPathSupplier implements TargetDataSetFileSupplier {

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
    public DataSetFile getTarget(DataSetFile sourceDataSetFile) throws DataSetException {
        return sourceDataSetFile.withNewPath(this.basepath);
    }
}
