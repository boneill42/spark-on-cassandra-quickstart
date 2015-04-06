package com.github.boneill42;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.UUID;

public class Sale {
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
}
