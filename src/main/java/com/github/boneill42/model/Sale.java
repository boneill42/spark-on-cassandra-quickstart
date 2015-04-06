package com.github.boneill42.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Sale implements Serializable {
    private static final long serialVersionUID = 1L;
    private UUID id;
    private Integer product;
    private BigDecimal price;

    public Sale() {
    }

    public Sale(UUID id, Integer product, BigDecimal price) {
        this.id = id;
        this.product = product;
        this.price = price;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Integer getProduct() {
        return product;
    }

    public void setProduct(Integer product) {
        this.product = product;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Sale'{'id={0}, product={1}, price={2}'}'", id, product, price);
    }
    
    public static List<String> columns() {
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        columns.add("product");
        columns.add("price");
        return columns;
    }
}
