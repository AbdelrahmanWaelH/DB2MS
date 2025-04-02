package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable
{
	ArrayList<Page> pages;
	String[] columns;
	int columnNumber;
	int tablePageCount;
	public Table(String [] columns){
		pages = null;
		tablePageCount = 0;
		this.columns = columns;
		columnNumber = columns.length;
	}
	public void addPage(){
		this.pages.add(new Page());
		this.tablePageCount++;
	}

	public int getLastPageNumOfRecords(){
		return getLastPage().getNumOfRecords();
	}

	public Page getLastPage(){
		if (pages == null){
			addPage();
		}
		return pages.get(tablePageCount - 1);
	}
	public boolean canInsertIntoLastPage(int dataPageSize){
		return getLastPageNumOfRecords() < dataPageSize;
	}
	public void insertRecord(String[] row){
		getLastPage().insertRow(row);
	}
	public int getLastPageNumber(){
		return pages.size()-1;
	}
}
