package com.github.boneill42.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class Product implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer id;
    private String name;
    private List<Integer> parents;

    public Product() {
    }

    public Product(Integer id, String name, List<Integer> parents) {
        this.id = id;
        this.name = name;
        this.parents = parents;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getParents() {
        return parents;
    }

    public void setParents(List<Integer> parents) {
        this.parents = parents;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Product'{'id={0}, name=''{1}'', parents={2}'}'", id, name, parents);
    }
    
    public static List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("id");
        columns.add("name");
        columns.add("parents"); 
        return columns;
    }

}
