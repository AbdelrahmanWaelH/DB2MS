package com.dbapp;

import java.io.Serializable;
import java.util.Map;

public class BitmapIndex implements Serializable {
    // stores the index as a map of the value and the index in binary
    // <a> -> <01110001001000111011011011>
    // <b> -> <10001110110111000100100100>
    private Map<String,String> index;
    private String columnName;
    private String tableName;
    private int columnIndex;
    
    // Getters
    public Map<String, String> getIndex() {
        return index;
    }
    
    public String getColumnName() {
        return columnName;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public int getColumnIndex() {
        return columnIndex;
    }
    
    // Setters
    public void setIndex(Map<String, String> index) {
        this.index = index;
    }
    
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }
    
    // Constructors
    public BitmapIndex() {
    }
    
    public BitmapIndex(Map<String, String> index, String columnName, String tableName, int columnIndex) {
        this.index = index;
        this.columnName = columnName;
        this.tableName = tableName;
        this.columnIndex = columnIndex;
    }
    
    // Builder implementation
    public static BitmapIndexBuilder builder() {
        return new BitmapIndexBuilder();
    }
    
    public static class BitmapIndexBuilder {
        private Map<String, String> index;
        private String columnName;
        private String tableName;
        private int columnIndex;
        
        public BitmapIndexBuilder index(Map<String, String> index) {
            this.index = index;
            return this;
        }
        
        public BitmapIndexBuilder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }
        
        public BitmapIndexBuilder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        
        public BitmapIndexBuilder columnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
            return this;
        }
        
        public BitmapIndex build() {
            return new BitmapIndex(index, columnName, tableName, columnIndex);
        }
    }
}
