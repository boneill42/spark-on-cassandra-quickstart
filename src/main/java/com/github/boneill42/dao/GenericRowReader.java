package com.github.boneill42.dao;

import java.io.Serializable;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.rdd.reader.RowReader;

import scala.Option;
import scala.collection.Seq;

public abstract class GenericRowReader<T> implements RowReader<T>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Option<Seq<ColumnRef>> neededColumns() {
        return Option.empty();
    }

}
