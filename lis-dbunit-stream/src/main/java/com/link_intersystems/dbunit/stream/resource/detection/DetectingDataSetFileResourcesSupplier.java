package com.link_intersystems.dbunit.stream.resource.detection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.DataSetResourcesSupplier;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileLocations;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DetectingDataSetFileResourcesSupplier implements DataSetResourcesSupplier {

    private DataSetFileLocations fileLocations;
    private DataSetFileDetection fileDetection;

    public DetectingDataSetFileResourcesSupplier(DataSetFileLocations fileLocations, DataSetFileDetection fileDetection) {
        this.fileLocations = requireNonNull(fileLocations);
        this.fileDetection = requireNonNull(fileDetection);
    }

    @Override
    public List<DataSetResource> getDataSetResources() {
        List<File> dataSetMatches = fileLocations.getFiles();

        return resolveDataSetMigrations(dataSetMatches);
    }

    private List<DataSetResource> resolveDataSetMigrations(List<File> dataSetMatches) {
        LinkedHashSet<DataSetFile> sourceDataSetFiles = new LinkedHashSet<>();

        for (File dataSetMatch : dataSetMatches) {
            DataSetFile dataSetFile = fileDetection.detect(dataSetMatch);
            if (dataSetFile != null) {
                sourceDataSetFiles.add(dataSetFile);
            }
        }

        return new ArrayList<>(sourceDataSetFiles);
    }
}
