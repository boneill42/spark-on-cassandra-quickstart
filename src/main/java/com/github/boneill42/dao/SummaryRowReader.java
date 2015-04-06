package com.github.boneill42.dao;

import scala.Option;
import scala.collection.Seq;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.rdd.reader.RowReaderOptions;
import com.github.boneill42.model.Summary;

public class SummaryRowReader extends GenericRowReader<Summary> {
    private static final long serialVersionUID = 1L;
    private static RowReader<Summary> reader = new SummaryRowReader();

    public static class SummaryRowReaderFactory implements RowReaderFactory<Summary>{
        @Override
        public RowReader<Summary> rowReader(TableDef arg0, RowReaderOptions arg1) {
            return reader;
        }

        @Override
        public RowReaderOptions rowReader$default$2() {
            return null;
        }

        @Override
        public Class<Summary> targetClass() {
            return Summary.class;
        }        
    }

    @Override
    public Option<Seq<String>> columnNames() {
        Seq<String> seq = scala.collection.JavaConversions.asScalaBuffer(Summary.columns()).toList();
        return Option.apply(seq);
    }

    @Override
    public Summary read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
        Summary summary = new Summary();
        summary.setProduct(row.getInt(0));
        summary.setSummary(row.getDecimal(1));
        return summary;
    }
}
