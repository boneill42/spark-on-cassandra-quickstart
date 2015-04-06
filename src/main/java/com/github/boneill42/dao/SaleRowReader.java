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
import com.github.boneill42.model.Sale;

public class SaleRowReader extends GenericRowReader<Sale> {
    private static final long serialVersionUID = 1L;
    private static RowReader<Sale> reader = new SaleRowReader();

    public static class SaleRowReaderFactory implements RowReaderFactory<Sale>, Serializable{
        private static final long serialVersionUID = 1L;
        @Override
        public RowReader<Sale> rowReader(TableDef arg0, RowReaderOptions arg1) {
            return reader;
        }

        @Override
        public RowReaderOptions rowReader$default$2() {
            return null;
        }

        @Override
        public Class<Sale> targetClass() {
            return Sale.class;
        }        
    }

    @Override
    public Option<Seq<String>> columnNames() {
        Seq<String> seq = scala.collection.JavaConversions.asScalaBuffer(Sale.columns()).toList();
        return Option.apply(seq);
    }

    @Override
    public Sale read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
        Sale sale = new Sale();
        sale.setId(row.getUUID(0));
        sale.setPrice(row.getDecimal(1));
        sale.setProduct(row.getInt(2));
        return sale;
    }
}
