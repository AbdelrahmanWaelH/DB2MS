package com.dbapp;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{
	private static final long serialVersionUID = 1L;
    ArrayList<String []> records;

    public Page(){
        this.records = new ArrayList<>();
    }

    public boolean isFull(){
        return (records.size() >= DBApp.dataPageSize);
    }
}
