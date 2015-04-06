package com.github.boneill42;

import java.math.BigDecimal;
import java.text.MessageFormat;

public class Summary {
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
}
