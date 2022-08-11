package com.link_intersystems.dbunit.stream.resource.detection.file.xls;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetDetector implements DataSetFileDetector {

    private Logger logger = LoggerFactory.getLogger(XlsDataSetDetector.class);

    private DataSetFileDetector implementor;

    public XlsDataSetDetector() {
        try {
            implementor = createXlsDetectorImplementor();
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            String msg = "XlsFileDetector is out of order. Maybe the apache poi library is not on the classpath.";
            if (logger.isDebugEnabled()) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
        }
    }

    @Override
    public DataSetFile detect(File file) {
        if (implementor != null) {
            return implementor.detect(file);
        }
        return null;
    }

    private DataSetFileDetector createXlsDetectorImplementor() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName("org.apache.poi.ss.usermodel.Workbook");
        Class<?> implementor = Class.forName("com.link_intersystems.dbunit.stream.resource.detection.file.xls.XlsDataSetDetectorImplementor");
        Constructor<?> declaredConstructor = implementor.getDeclaredConstructor();
        return (DataSetFileDetector) declaredConstructor.newInstance();
    }


}
