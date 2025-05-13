package com.dbapp;

import java.io.Serializable;
import java.util.List;

public class Table implements Serializable {
    private static final long serialVersionUID = 1L;
    private String[] columnsNames;
    private List<String> traces;
    int pageCount;
    
    // Getters
    public String[] getColumnsNames() {
        return columnsNames;
    }
    
    public List<String> getTraces() {
        return traces;
    }
    
    public int getPageCount() {
        return pageCount;
    }
    
    // Setters
    public void setColumnsNames(String[] columnsNames) {
        this.columnsNames = columnsNames;
    }
    
    public void setTraces(List<String> traces) {
        this.traces = traces;
    }
    
    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }
    
    // Constructors
    public Table() {
    }
    
    public Table(String[] columnsNames, List<String> traces, int pageCount) {
        this.columnsNames = columnsNames;
        this.traces = traces;
        this.pageCount = pageCount;
    }
    
    // Builder implementation
    public static TableBuilder builder() {
        return new TableBuilder();
    }
    
    public static class TableBuilder {
        private String[] columnsNames;
        private List<String> traces;
        private int pageCount;
        
        public TableBuilder columnsNames(String[] columnsNames) {
            this.columnsNames = columnsNames;
            return this;
        }
        
        public TableBuilder traces(List<String> traces) {
            this.traces = traces;
            return this;
        }
        
        public TableBuilder pageCount(int pageCount) {
            this.pageCount = pageCount;
            return this;
        }
        
        public Table build() {
            return new Table(columnsNames, traces, pageCount);
        }
    }
}
