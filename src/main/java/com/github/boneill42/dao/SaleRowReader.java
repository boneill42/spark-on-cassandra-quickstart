package com.github.boneill42.dao;

import java.io.Serializable;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.github.boneill42.model.Sale;

import scala.collection.IndexedSeq;

public class SaleRowReader extends GenericRowReader<Sale> {
    private static final long serialVersionUID = 1L;
    private static RowReader<Sale> reader = new SaleRowReader();

    public static class SaleRowReaderFactory implements RowReaderFactory<Sale>, Serializable{
        private static final long serialVersionUID = 1L;
        @Override
        public RowReader<Sale> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return reader;
        }        

        @Override
        public Class<Sale> targetClass() {
            return Sale.class;
        }
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
