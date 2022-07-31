package com.link_intersystems.dbunit.stream.resource.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileDetection {

    private List<DataSetFileDetector> detectors;
    private DataSetFileConfig dataSetFileConfig = new DataSetFileConfig();

    /**
     * @param detectors the {@link DataSetFileDetector}s to use. If none are set (null is the default)
     *                  the {@link DataSetFileDetector}s are looked up via
     *                  META-INF/services/com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetectorProvider.
     */
    public void setDetectors(List<DataSetFileDetector> detectors) {
        this.detectors = detectors;
    }

    public void setDataSetFileConfig(DataSetFileConfig dataSetFileConfig) {
        this.dataSetFileConfig = requireNonNull(dataSetFileConfig);
    }

    private List<DataSetFileDetector> getDetectors() {
        if (detectors != null) {
            return detectors;
        }

        List<DataSetFileDetectorProvider> providers = new ArrayList<>();
        ServiceLoader<DataSetFileDetectorProvider> dataSetDetectorsLoader = ServiceLoader.load(DataSetFileDetectorProvider.class, Thread.currentThread().getContextClassLoader());
        dataSetDetectorsLoader.forEach(providers::add);

        return providers.stream()
                .map(p -> p.getDataSetFileDetector(dataSetFileConfig))
                .collect(toList());
    }

    public DataSetFile detect(File file) {
        for (DataSetFileDetector detector : getDetectors()) {
            DataSetFile dataSetFile = detector.detect(file);
            if (dataSetFile != null) {
                return dataSetFile;
            }
        }
        return null;
    }
}
