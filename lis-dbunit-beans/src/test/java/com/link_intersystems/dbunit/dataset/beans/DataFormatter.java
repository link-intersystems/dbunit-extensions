package com.link_intersystems.dbunit.dataset.beans;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataFormatter {

    public static final DataFormatter YYYY_MM_DD = new DataFormatter();

    private SimpleDateFormat format;

    public DataFormatter() {
        this(new SimpleDateFormat("yyyy-MM-dd"));
    }

    public DataFormatter(SimpleDateFormat dateFormat) {
        this.format = dateFormat;
    }


    public Date parse(String yyyyMMdd) throws ParseException {
        return format.parse(yyyyMMdd);
    }

    public Date safeParse(String yyyyMMDD) {
        try {
            return parse(yyyyMMDD);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}