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
		pages = new ArrayList<Page>();
		tablePageCount = 0;
		this.columns = columns;
		columnNumber = columns.length;
	}
	public void addPage(){
		if(pages == null){
			pages = new ArrayList<>();
		}
		this.pages.add(new Page());
		this.tablePageCount++;
	}

	public int getLastPageNumOfRecords(){
		if (pages!= null)
			return getLastPage().getNumOfRecords();
		return 0;
	}

	public Page getLastPage(){
		if (pages == null || pages.isEmpty()){
			addPage();
		}
		return pages.get(pages.size()-1 );
	}
	public boolean canInsertIntoLastPage(int dataPageSize){

		return getLastPageNumOfRecords() < dataPageSize;
	}
	public void insertRecord(String[] row){
		getLastPage().insertRow(row);
	}
	public int getLastPageNumber(){
		return pages.size();
	}
}
