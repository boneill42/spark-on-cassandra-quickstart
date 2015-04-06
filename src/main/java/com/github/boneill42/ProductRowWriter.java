package com.github.boneill42;

import java.util.ArrayList;
import java.util.List;

import scala.collection.Seq;
import scala.collection.immutable.Map;

import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

public class ProductRowWriter implements RowWriter<Product> {
    private static final long serialVersionUID = 1L;
    private static RowWriter<Product> writer = new ProductRowWriter();

    // Factory
    public static class ProductRowWriterFactory implements RowWriterFactory<Product>{
        @Override
        public RowWriter<Product> rowWriter(TableDef arg0, Seq<String> arg1, Map<String, String> arg2) {
            return writer;
        }        
    }

    @Override
    public Seq<String> columnNames() {
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        columns.add("name");
        columns.add("parents");        
        return scala.collection.JavaConversions.asScalaBuffer(columns).toList();
    }

    @Override
    public void readColumnValues(Product product, Object[] buffer) {
        buffer[0] = product.getId();
        buffer[1] = product.getName();
        buffer[2] = product.getParents();        
    }
    
}
