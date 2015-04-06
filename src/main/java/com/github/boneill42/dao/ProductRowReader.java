package com.github.boneill42.dao;

import java.io.Serializable;

import scala.Option;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;
import com.github.boneill42.model.Product;

public class ProductRowReader extends GenericRowReader<Product> {
    private static final long serialVersionUID = 1L;
    private static RowReader<Product> reader = new ProductRowReader();

    public static class ProductRowReaderFactory implements RowReaderFactory<Product>, Serializable{
        private static final long serialVersionUID = 1L;

        @Override
        public RowReader<Product> rowReader(TableDef arg0, RowReaderOptions arg1) {
            return reader;
        }

        @Override
        public RowReaderOptions rowReader$default$2() {
            return null;
        }

        @Override
        public Class<Product> targetClass() {
            return Product.class;
        }        
    }

    @Override
    public Option<Seq<String>> columnNames() {
        Seq<String> seq = scala.collection.JavaConversions.asScalaBuffer(Product.columns()).toList();
        return Option.apply(seq);
    }


    @Override
    public Product read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
        Product product = new Product();        
        product.setId(row.getInt(0));
        product.setName(row.getString(1));
        product.setParents(row.getList(2, Integer.class));
        return product;
    }
}
