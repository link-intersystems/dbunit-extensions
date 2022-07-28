package com.link_intersystems.dbunit.stream.resource.file.xls;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

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
    public DataSetFile detect(Path path) {
        if (implementor != null) {
            return implementor.detect(path);
        }
        return null;
    }

    private DataSetFileDetector createXlsDetectorImplementor() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName("org.apache.poi.ss.usermodel.Workbook");
        Class<?> implementor = Class.forName("com.link_intersystems.dbunit.stream.resource.file.xls.XlsDataSetDetectorImplementor");
        Constructor<?> declaredConstructor = implementor.getDeclaredConstructor();
        return (DataSetFileDetector) declaredConstructor.newInstance();
    }


}
