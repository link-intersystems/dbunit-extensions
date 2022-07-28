package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.io.FilePath;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileDetection {

    private List<DataSetFileDetector> detectors;

    public void setDetectors(List<DataSetFileDetector> detectors) {
        this.detectors = detectors;
    }

    private List<DataSetFileDetector> getDetectors() {
        if (detectors != null) {
            return detectors;
        }

        List<DataSetFileDetector> dataSetDetectors = new ArrayList<>();
        ServiceLoader<DataSetFileDetector> dataSetDetectorsLoader = ServiceLoader.load(DataSetFileDetector.class, Thread.currentThread().getContextClassLoader());
        dataSetDetectorsLoader.forEach(dataSetDetectors::add);
        return dataSetDetectors;
    }

    public DataSetFile detect(Path path) {
        for (DataSetFileDetector detector : getDetectors()) {
            DataSetFile dataSetFile = detector.detect(path);
            if (dataSetFile != null) {
                return dataSetFile;
            }
        }
        return null;
    }
}
