package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.DataSetResourcesSupplier;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetection;
import com.link_intersystems.io.FilePath;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetResourcesSupplier implements DataSetResourcesSupplier {

    private DataSetFileLocations fileLocations;
    private DataSetFileDetection fileDetection;

    public DefaultDataSetResourcesSupplier(DataSetFileLocations fileLocations, DataSetFileDetection fileDetection) {
        this.fileLocations = requireNonNull(fileLocations);
        this.fileDetection = requireNonNull(fileDetection);
    }

    @Override
    public List<DataSetResource> getDataSetResources() {
        List<FilePath> dataSetMatches = fileLocations.getPaths();

        return resolveDataSetMigrations(dataSetMatches);
    }

    private List<DataSetResource> resolveDataSetMigrations(List<FilePath> dataSetMatches) {
        List<DataSetFile> sourceDataSetFiles = new ArrayList<>();
        Set<DataSetFile> uniqueDataSetFiles = new HashSet<>();

        for (FilePath filePath : dataSetMatches) {
            Path absolutePath = filePath.toAbsolutePath();
            DataSetFile dataSetFile = fileDetection.detect(absolutePath);
            if (dataSetFile != null) {
                if (uniqueDataSetFiles.add(dataSetFile)) {
                    sourceDataSetFiles.add(dataSetFile);
                }
            }
        }

        return new ArrayList<>(sourceDataSetFiles);
    }
}
