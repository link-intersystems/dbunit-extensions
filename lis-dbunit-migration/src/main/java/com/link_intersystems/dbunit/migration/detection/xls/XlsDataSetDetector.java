package com.link_intersystems.dbunit.migration.detection.xls;

import com.link_intersystems.dbunit.migration.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetDetector implements DataSetFileDetector {
    @Override
    public DataSetFile detect(File file) {
        if(file.isDirectory()){
            return null;
        }

        if(file.getName().endsWith(".xls")){
            try(InputStream in = new BufferedInputStream(new FileInputStream(file))){
                Workbook workbook = WorkbookFactory.create(in);
                workbook.close();
                return new XlsDataSetFile(file);
            } catch (FileNotFoundException e) {
            } catch (IOException e) {
            }
        }

        return null;
    }
}