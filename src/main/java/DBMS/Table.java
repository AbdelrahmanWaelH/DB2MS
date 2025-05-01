package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Table implements Serializable
{
	String tableName;
	ArrayList<Page> pages;
	String[] columns;
	int columnNumber;
	int tablePageCount;
	ArrayList<String> Trace;
	boolean indices[];
	int recordsCount;

	public Table(String tableName,String [] columns){
		pages = new ArrayList<Page>();
		tablePageCount = 0;
		this.columns = columns;
		columnNumber = columns.length;
		this.tableName = tableName;
		indices = new boolean[columnNumber];
		Trace = new ArrayList<String>();
		Trace.add("Table created name:" + tableName +"ColumnsNames" + Arrays.toString(columns));
		FileManager.storeTable(tableName,this);
	}
	public String getFullTrace(int dataPageSize){
		Trace.add("Pages Count: "+this.pages.size()+", Records Count: "+this.recordsCount)	;
		return String.join("\n", Trace);
	}
	public String getLastTrace(){
		return Trace.get(Trace.size()- 1);
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
	public void insertRecord(String tableName,String[] row){
		long startTime = System.currentTimeMillis();
		getLastPage().insertRow(row);
		FileManager.storeTablePage(tableName,getLastPageNumber()-1,getLastPage());
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		this.recordsCount++;
		Trace.add("Inserted:"+Arrays.toString(row)+", at page number:"+this.getLastPageNumber() + ", execution time (mil):" +executionTime );
		FileManager.storeTable(tableName,this);
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
		long startTime = System.currentTimeMillis();

		ArrayList<String[]> ans = pages.stream()
				.flatMap(page -> page.getRows().stream())
				.collect(Collectors.toCollection(ArrayList::new));
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		Trace.add("Select all pages:"+pages.size()+", records:"+ans.size()+", execution time (mil):" + executionTime);
		FileManager.storeTable(tableName,this);
		return ans;

	}
	public String[] getRecord(String tableName,int pageNumber, int recordNumber){
		long startTime = System.currentTimeMillis();
		String [] ans = FileManager.loadTablePage(tableName,pageNumber).getRecord(recordNumber);


		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		Trace.add("Select pointer page:"+pageNumber+", record:"+recordNumber+", total output count:1, execution time (mil):" + executionTime);
		FileManager.storeTable(tableName,this);
		return ans;
	}

	public ArrayList<String[]> selectWhere( String[] cols, String[] vals){

		long startTime = System.currentTimeMillis();

		String[] tmp = new String[columns.length];
		ArrayList<String[] > ans = new ArrayList<String[]>();
		for (int i = 0; i < cols.length; i++) {
			for (int j = 0; j < columns.length; j++) {
				if(cols[i].equals(columns[j]))
					tmp[j] = vals[i];
			}
		}

		int records_per_page[] =  new int[pages.size()];
		for (int i = 0; i < pages.size(); i++) {

			ArrayList<String[]> pageRecords = pages.get(i).getRows();
			for (String[] pageRecord :
					pageRecords) {
				if(Compare(tmp,pageRecord )) {
					ans.add(pageRecord);
					records_per_page[i]++;
				}
			}
		}
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		StringBuilder formattedRecordsPerPage = new StringBuilder();
		formattedRecordsPerPage.append("[");
		boolean firstEntry = true;
		for (int i = 0; i < records_per_page.length; i++) {
			if (records_per_page[i] > 0) {
				if (!firstEntry) {
					formattedRecordsPerPage.append(", ");
				}
				formattedRecordsPerPage.append("[").append(i).append(", ").append(records_per_page[i]).append("]");
				firstEntry = false;
			}
		}
		formattedRecordsPerPage.append("]");
		Trace.add( "Select condition(s):"+Arrays.toString(cols)+"->"+Arrays.toString(vals) +", Records per page:"+formattedRecordsPerPage + ", records:"+ans.size() +", execution time (mil):"+ executionTime);
		FileManager.storeTable(tableName,this);
		return ans;
	}
	public static boolean Compare(String[] a, String[] b){
		for (int i = 0; i < a.length; i++) {
			if (a[i] != null  && ! a[i].equals(b[i]))
				return false;
		}
		return true;
	}
}

