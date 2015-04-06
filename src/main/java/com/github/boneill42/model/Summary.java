package com.github.boneill42.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class Summary implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer product;
    private BigDecimal summary;

    public Summary() {
    }

    public Summary(Integer product, BigDecimal summary) {
        this.product = product;
        this.summary = summary;
    }

    public Integer getProduct() {
        return product;
    }

    public void setProduct(Integer product) {
        this.product = product;
    }

    public BigDecimal getSummary() {
        return summary;
    }

    public void setSummary(BigDecimal summary) {
        this.summary = summary;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Summary'{'product={0}, summary={1}'}'", product, summary);
    }
    
    public static List<String> columns() {
        List<String> columns = new ArrayList<String>();
        columns.add("product");
        columns.add("summary");
        return columns;
    }
}
