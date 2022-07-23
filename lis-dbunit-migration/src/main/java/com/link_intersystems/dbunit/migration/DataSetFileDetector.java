package com.link_intersystems.dbunit.migration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileDetector {

    private List<DataSetDetector> detectors;

    public void setDetectors(List<DataSetDetector> detectors) {
        this.detectors = detectors;
    }

    private List<DataSetDetector> getDetectors() {
        if (detectors != null) {
            return detectors;
        }

        List<DataSetDetector> dataSetDetectors = new ArrayList<>();
        ServiceLoader<DataSetDetector> dataSetDetectorsLoader = ServiceLoader.load(DataSetDetector.class, Thread.currentThread().getContextClassLoader());
        dataSetDetectorsLoader.forEach(dataSetDetectors::add);
        return dataSetDetectors;
    }

    public DataSetFile detect(File file) {
        for (DataSetDetector detector : getDetectors()) {
            DataSetFile dataSetFile = detector.detect(file);
            if (dataSetFile != null) {
                return dataSetFile;
            }
        }
        return null;
    }
}
