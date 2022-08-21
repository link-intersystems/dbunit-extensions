package com.link_intersystems.dbunit.stream.consumer.xls;

import org.apache.poi.ss.usermodel.*;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.excel.XlsDataSetWriter;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Date;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetConsumer extends XlsDataSetWriter implements IDataSetConsumer {


    private OutputStream outputStream;
    private CellStyle dateCellStyle;
    private Workbook workbook;
    private Sheet tableSheet;
    private int tableSheetIndex;
    private int tableRowIndex;
    private ITableMetaData metaData;

    public XlsDataSetConsumer(OutputStream outputStream) {
        this.outputStream = requireNonNull(outputStream);
    }

    @Override
    public void startDataSet() {
        workbook = this.createWorkbook();
        this.dateCellStyle = createDateCellStyle(workbook);
        tableSheetIndex = 0;
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            workbook.write(outputStream);
            outputStream.flush();
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        this.metaData = metaData;
        tableSheet = workbook.createSheet(this.metaData.getTableName());
        workbook.setSheetName(tableSheetIndex++, metaData.getTableName());

        Row headerRow = tableSheet.createRow(0);
        Column[] columns = metaData.getColumns();

        int j;
        for (j = 0; j < columns.length; ++j) {
            Column column = columns[j];
            Cell cell = headerRow.createCell(j);
            cell.setCellValue(column.getColumnName());
        }

        tableRowIndex = 1;
    }

    @Override
    public void endTable() {
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        Row row = tableSheet.createRow(tableRowIndex++);
        for (int i = 0; i < objects.length; i++) {
            Object value = objects[i];
            if (value != null) {
                Cell cell = row.createCell(i);
                if (value instanceof Date) {
                    this.setDateCell(cell, (Date) value, workbook);
                } else if (value instanceof BigDecimal) {
                    this.setNumericCell(cell, (BigDecimal) value, workbook);
                } else if (value instanceof Long) {
                    this.setDateCell(cell, new Date((Long) value), workbook);
                } else {
                    cell.setCellValue(DataType.asString(value));
                }
            }
        }
    }

    protected void setDateCell(Cell cell, Date value, Workbook workbook) {
        long timeMillis = value.getTime();
        cell.setCellValue((double) timeMillis);
        cell.setCellType(CellType.NUMERIC);
        cell.setCellStyle(this.dateCellStyle);
    }
}
