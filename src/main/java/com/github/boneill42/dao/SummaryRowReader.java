package com.github.boneill42.dao;

import java.io.Serializable;

import com.datastax.driver.core.Row;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.github.boneill42.model.Summary;

import scala.collection.IndexedSeq;

public class SummaryRowReader extends GenericRowReader<Summary> {
    private static final long serialVersionUID = 1L;
    private static RowReader<Summary> reader = new SummaryRowReader();

    public static class SummaryRowReaderFactory implements RowReaderFactory<Summary>, Serializable{
        private static final long serialVersionUID = 1L;

        @Override
        public RowReader<Summary> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return reader;
        }

        @Override
        public Class<Summary> targetClass() {
            return Summary.class;
        }
    }

    @Override
    public Summary read(Row row, String[] columnNames) {
        Summary summary = new Summary();
        summary.setProduct(row.getInt(0));
        summary.setSummary(row.getDecimal(1));
        return summary;
    }
}
