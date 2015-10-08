package com.github.boneill42.dao;

import java.io.Serializable;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.github.boneill42.model.Summary;

import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class SummaryRowWriter implements RowWriter<Summary> {
    private static final long serialVersionUID = 1L;
    private static RowWriter<Summary> writer = new SummaryRowWriter();

    // Factory
    public static class SummaryRowWriterFactory implements RowWriterFactory<Summary>, Serializable{
        private static final long serialVersionUID = 1L;

        @Override
        public RowWriter<Summary> rowWriter(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return writer;
        }
    }

    @Override
    public Seq<String> columnNames() {
        return scala.collection.JavaConversions.asScalaBuffer(Summary.columns()).toList();
    }
    
    @Override
    public void readColumnValues(Summary summary, Object[] buffer) {
        buffer[0] = summary.getProduct();
        buffer[1] = summary.getSummary();
    }
    
}
