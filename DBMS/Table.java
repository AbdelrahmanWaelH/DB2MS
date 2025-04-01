package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable
{
	private static final long serialVersionUID = 1L;
	ArrayList<Page> pages;
	ArrayList<String> traceLog;
	String[] columnsNames;
	
	public Table(String[] columnsNames){
		this.columnsNames = columnsNames;
		this.pages = new ArrayList<>();
		this.traceLog = new ArrayList<>();
	}
}
