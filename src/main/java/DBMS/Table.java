package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

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
	/* my initial implementation:
	public ArrayList<String[]> getAllTableContent(){
		ArrayList<String[]> tmp  =new ArrayList<String[]>();
		for (Page page :
				pages) {
			ArrayList<String[]> pageRows= page.getRows();
			for (String[] row:
			pageRows){
				tmp.add(row);
			}
		}
		return tmp;
	}

	 */
	// claudes implementation:
	public ArrayList<String[]> getAllTableContent() {
		return pages.stream()
				.flatMap(page -> page.getRows().stream())
				.collect(Collectors.toCollection(ArrayList::new));
	}
	public String[] getRecord(int pageNumber, int recordNumber){
		return pages.get(pageNumber).getRecord(recordNumber);
	}
}

