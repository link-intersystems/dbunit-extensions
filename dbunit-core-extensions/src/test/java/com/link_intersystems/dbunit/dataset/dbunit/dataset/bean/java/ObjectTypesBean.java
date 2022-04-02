package com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.java;

import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.java.PrimitiveTypesBean;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

abstract class ObjectTypesBean {

    public abstract String getStringValue();

    public abstract CharSequence getCharSequenceValue();

    public abstract Date getDateValue();

    public abstract BigInteger getBigIntegerValue();

    public abstract BigDecimal getBigDecimalValue();

    public abstract PrimitiveTypesBean getUnknownType();
}
