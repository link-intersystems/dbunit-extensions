package com.link_intersystems.dbunit.dataset.consumer;

import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaSlimTestDBExtension.class)
class XlsDataSetConsumerTest {


    private XlsDataSetConsumer dataSetConsumer;
    private ByteArrayOutputStream bout;
    private IDataSet sakilaDataSet;
    private DatabaseDataSet databaseDataSet;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException, SQLException {
        bout = new ByteArrayOutputStream();
        dataSetConsumer = new XlsDataSetConsumer(bout);

        databaseDataSet = new DatabaseDataSet(new DatabaseConnection(connection, "sakila"), false);
        sakilaDataSet = new FilteredDataSet(SakilaTinyDB.getTableNames().toArray(new String[0]), databaseDataSet);
    }

    @Test
    void createExcelFile() throws DataSetException, IOException {

        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(sakilaDataSet);
        producerAdapter.setConsumer(dataSetConsumer);
        producerAdapter.produce();


        HSSFWorkbook workbook = new HSSFWorkbook(new ByteArrayInputStream(bout.toByteArray()));
        assertWorkbook(sakilaDataSet, workbook);
    }

    private void assertWorkbook(IDataSet expectedDataSet, HSSFWorkbook workbook) throws DataSetException {
        int numberOfSheets = workbook.getNumberOfSheets();
        assertEquals(expectedDataSet.getTableNames().length, numberOfSheets);


        String[] tableNames = expectedDataSet.getTableNames();
        for (String tableName : tableNames) {
            HSSFSheet sheet = workbook.getSheet(tableName);
            ITable table = expectedDataSet.getTable(tableName);
            assertSheet(table, sheet);
        }
    }

    private void assertSheet(ITable table, HSSFSheet sheet) throws DataSetException {
        String tableName = table.getTableMetaData().getTableName();
        String sheetName = sheet.getSheetName();
        assertEquals(tableName, sheetName);

        Iterator<Row> rowIterator = sheet.iterator();
        TableUtil tableUtil = new TableUtil(table);

        assertHeader(table.getTableMetaData(), rowIterator.next());

        int rowIndex = 0;
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            com.link_intersystems.dbunit.table.Row tableRow = tableUtil.getRow(rowIndex++);
            assertRow(table.getTableMetaData(), tableRow, row);

        }

    }

    private void assertHeader(ITableMetaData tableMetaData, Row headerRow) throws DataSetException {
        Column[] columns = tableMetaData.getColumns();

        for (int i = 0; i < columns.length; i++) {
            String stringCellValue = headerRow.getCell(i).getStringCellValue();
            assertEquals(columns[i].getColumnName(), stringCellValue);
        }
    }

    private void assertRow(ITableMetaData tableMetaData, com.link_intersystems.dbunit.table.Row tableRow, Row sheetRow) throws DataSetException {
        for (int columnIndex = 0; columnIndex < tableRow.size(); columnIndex++) {
            Object columnValue = tableRow.get(columnIndex);
            Cell cell = sheetRow.getCell(columnIndex);

            Column column = tableMetaData.getColumns()[columnIndex];
            assertCell(column, columnValue, cell);
        }
    }

    private void assertCell(Column column, Object columnValue, Cell cell) {
        String tableValue = String.valueOf(columnValue);
        String cellValue = "null";
        if (cell != null) {
            CellType cellType = cell.getCellType();
            switch (cellType) {
                case NUMERIC:
                    double numericCellValue = cell.getNumericCellValue();
                    if (column.getDataType().equals(DataType.TIMESTAMP)) {
                        Date date = new Date((long) numericCellValue);
                        cellValue = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(date);
                    } else if (column.getDataType().equals(DataType.DATE)) {
                        Date date = new Date((long) numericCellValue);
                        cellValue = new SimpleDateFormat("yyyy-MM-dd").format(date);
                    } else {
                        cellValue = Double.toString(numericCellValue);
                    }
                    break;
                default:
                    cellValue = cell.getStringCellValue();
                    break;
            }
        }

        assertEquals(tableValue, cellValue);
    }

}