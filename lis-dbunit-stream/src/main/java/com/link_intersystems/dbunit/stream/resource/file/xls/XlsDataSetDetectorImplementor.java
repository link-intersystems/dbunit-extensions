package com.link_intersystems.dbunit.stream.resource.file.xls;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetector;
import com.link_intersystems.io.FilePath;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.MessageFormat;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetDetectorImplementor implements DataSetFileDetector {

    private Logger logger = LoggerFactory.getLogger(XlsDataSetDetectorImplementor.class);

    @Override
    public DataSetFile detect(FilePath filePath) {
        File file = filePath.toAbsoluteFile();
        if (file.isDirectory()) {
            return null;
        }

        if (file.getName().endsWith(".xls")) {
            try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                Workbook workbook = WorkbookFactory.create(in);
                workbook.close();
                return new XlsDataSetFile(filePath);
            } catch (Exception e) {
                logFileNotReadable(file, e);
            }
        }

        return null;
    }

    private void logFileNotReadable(File file, Exception e) {
        if (logger.isDebugEnabled()) {
            String msg = MessageFormat.format("File ''{}'' with extension xls, doesn't seem to be an excel file.", file);
            logger.debug(msg, e);
        }
    }
}
