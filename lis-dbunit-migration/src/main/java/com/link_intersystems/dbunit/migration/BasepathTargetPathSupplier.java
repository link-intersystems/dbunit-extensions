package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import org.dbunit.dataset.DataSetException;

import java.nio.file.Path;

import static java.text.MessageFormat.format;
import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BasepathTargetPathSupplier implements TargetDataSetResourceSupplier {

    private Path sourceBasepath;
    private Path targetBasepath;

    public BasepathTargetPathSupplier(Path sourceBasepath, Path targetBasepath) {
        this.sourceBasepath = requireNonNull(sourceBasepath);
        this.targetBasepath = requireNonNull(targetBasepath);
    }

    @Override
    public DataSetResource getTargetDataSetResource(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetFile sourceDataSetFile = sourceDataSetResource.getAdapter(DataSetFile.class);
        if (sourceDataSetFile != null) {
            Path sourceDataSetPath = sourceDataSetFile.getPath();

            Path relativized = sourceBasepath.relativize(sourceDataSetPath);
            Path resolved = targetBasepath.resolve(relativized);

            return sourceDataSetFile.withNewPath(resolved);
        }
        String mst = format("Unable to handle DataSetResource ''{0}''.", sourceDataSetResource);
        throw new IllegalStateException(mst);
    }
}
