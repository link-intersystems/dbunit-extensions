package com.link_intersystems.dbunit.stream.resource.detection;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.util.config.properties.ConfigProperties;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileDetection {

    private List<DataSetFileDetector> detectors;
    private ConfigProperties configProperties = new ConfigProperties();

    /**
     * @param detectors the {@link DataSetFileDetector}s to use. If none are set (null is the default)
     *                  the {@link DataSetFileDetector}s are looked up via
     *                  META-INF/services/com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetectorProvider.
     */
    public void setDetectors(List<DataSetFileDetector> detectors) {
        this.detectors = detectors;
    }

    public void setConfigProperties(ConfigProperties configProperties) {
        this.configProperties = requireNonNull(configProperties);
    }

    private List<DataSetFileDetector> getDetectors() {
        if (detectors != null) {
            return detectors;
        }

        List<DataSetFileDetectorProvider> providers = new ArrayList<>();
        ServiceLoader<DataSetFileDetectorProvider> dataSetDetectorsLoader = ServiceLoader.load(DataSetFileDetectorProvider.class, Thread.currentThread().getContextClassLoader());
        dataSetDetectorsLoader.forEach(providers::add);

        return providers.stream()
                .map(p -> p.getDataSetFileDetector(configProperties))
                .sorted(this::compare)
                .collect(toList());
    }

    private int compare(DataSetFileDetector detector1, DataSetFileDetector detector2) {
        Order order1 = findOrder(detector1);
        Order order2 = findOrder(detector2);

        Comparator<Integer> comparator = Integer::compare;
        return comparator.reversed().compare(order1.value(), order2.value());
    }

    private Order findOrder(DataSetFileDetector detector) {
        Class<? extends DataSetFileDetector> aClass = detector.getClass();
        Order order = findOrder(aClass);
        if (order == null) {
            order = new Order() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return Order.class;
                }

                @Override
                public int value() {
                    return 0;
                }
            };
        }
        return order;
    }

    private Order findOrder(Class<?> clazz) {
        if (clazz == null) {
            return null;
        }

        Order order = clazz.getAnnotation(Order.class);
        if (order == null) {
            return findOrder(clazz.getSuperclass());
        }

        return order;
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
