package com.github.boneill42.dao;

import java.io.Serializable;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.github.boneill42.model.Sale;

import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class SaleRowWriter implements RowWriter<Sale> {
    private static final long serialVersionUID = 1L;
    private static RowWriter<Sale> writer = new SaleRowWriter();

    // Factory
    public static class SaleRowWriterFactory implements RowWriterFactory<Sale>, Serializable{
        private static final long serialVersionUID = 1L;

        @Override
        public RowWriter<Sale> rowWriter(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return writer;
        }
    }

    @Override
    public Seq<String> columnNames() {
        return scala.collection.JavaConversions.asScalaBuffer(Sale.columns()).toList();
    }
    
    @Override
    public void readColumnValues(Sale sale, Object[] buffer) {
        buffer[0] = sale.getId();
        buffer[1] = sale.getProduct();
        buffer[2] = sale.getPrice();     
    }
    
}
