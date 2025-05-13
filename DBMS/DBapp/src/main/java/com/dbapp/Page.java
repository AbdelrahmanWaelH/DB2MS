package com.dbapp;

import java.io.Serializable;
import java.util.List;

public class Page implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<String[]> rows;
    
    // Getter
    public List<String[]> getRows() {
        return rows;
    }
    
    // Setter
    public void setRows(List<String[]> rows) {
        this.rows = rows;
    }
    
    // Constructors
    public Page() {
    }
    
    public Page(List<String[]> rows) {
        this.rows = rows;
    }
    
    public boolean isFull(int maxRows) {
        return rows.size() == maxRows;
    }
    
    // Builder implementation
    public static PageBuilder builder() {
        return new PageBuilder();
    }
    
    public static class PageBuilder {
        private List<String[]> rows;
        
        public PageBuilder rows(List<String[]> rows) {
            this.rows = rows;
            return this;
        }
        
        public Page build() {
            return new Page(rows);
        }
    }
}
