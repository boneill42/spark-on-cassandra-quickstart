package com.github.boneill42.dao;

import java.io.Serializable;

import scala.Option;

import com.datastax.spark.connector.rdd.reader.RowReader;

public abstract class GenericRowReader<T> implements RowReader<T>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Option<Object> consumedColumns() {
        return Option.empty();
    }
    
    @Override
    public Option<Object> requiredColumns() {
        return Option.empty();
    }
}
