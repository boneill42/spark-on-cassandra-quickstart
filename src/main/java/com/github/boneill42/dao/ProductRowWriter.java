package com.github.boneill42.dao;

import java.io.Serializable;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.github.boneill42.model.Product;

import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class ProductRowWriter implements RowWriter<Product> {
    private static final long serialVersionUID = 1L;
    private static RowWriter<Product> writer = new ProductRowWriter();

    // Factory
    public static class ProductRowWriterFactory implements RowWriterFactory<Product>, Serializable{
        private static final long serialVersionUID = 1L;
        @Override
        public RowWriter<Product> rowWriter(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return writer;
        }        
    }

    @Override
    public Seq<String> columnNames() {       
        return scala.collection.JavaConversions.asScalaBuffer(Product.columns()).toList();
    }

    @Override
    public void readColumnValues(Product product, Object[] buffer) {
        buffer[0] = product.getId();
        buffer[1] = product.getName();
        buffer[2] = product.getParents();        
    }
}
