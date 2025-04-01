package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

public class DBApp
{
	static int dataPageSize = 100;
	
	public static void createTable(String tableName, String[] columnsNames)
	{
		Table t = new Table(columnsNames);
		String startLog = String.format("Table created with name: '%s' & column names: '%s'", tableName, Arrays.toString(columnsNames));
		t.traceLog.add(startLog);
		FileManager.storeTable(tableName, t);
	}
	
	public static void insert(String tableName, String[] record)
	{
		Table t = FileManager.loadTable(tableName);
		Page insertionPage = null;
		int insertionPageNumber = 0;
		if (t == null) {
			System.out.println("Table with name: " +  tableName + " does not exist!");
			return;
		} else if (t.pages.isEmpty()){
			insertionPage = new Page();
			insertionPage.records.add(record);
			insertionPageNumber = 0;
		} else {

			Page lastPage = t.pages.get(t.pages.size() -1);
			
			if (lastPage.isFull()){
				Page extraPage = new Page();
				extraPage.records.add(record);
				t.pages.add(extraPage);
				insertionPage = extraPage;
				insertionPageNumber = t.pages.size()-1;
			} else {
				insertionPage = lastPage;
				lastPage.records.add(record);
				insertionPageNumber = t.pages.size();
			}	//record added to the appropriate page, insertion page and it's number are also ready
			FileManager.storeTablePage(tableName, insertionPageNumber, insertionPage);
			FileManager.storeTable(tableName, t);
			String entry = String.format("Inserted: %s, at page number:%d, at time: %d", Arrays.toString(record), insertionPageNumber, System.currentTimeMillis());
			t.traceLog.add(entry);
			FileManager.storeTable(tableName, t);
		}
	}
	
	public static ArrayList<String []> select(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> resultList = null;
		if (t == null){
			System.out.println("Table with name: " + tableName + " does not exist!");
			resultList = null;
		} else {
			resultList = new ArrayList<>();
			for (Page page : t.pages) {
				for (String[] record : page.records) {
					resultList.add(record);
				}
			}
		}
		String entry = String.format("Entire table selected at: %d", System.currentTimeMillis());
		t.traceLog.add(entry);
		FileManager.storeTable(tableName, t);
		return resultList;
	}
	
	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> resultList = null;
		if (t == null){
			System.out.println("Table with name: " + tableName + " does not exist!");
			resultList = null;
		} else {
			resultList = new ArrayList<>();
			Page p = t.pages.get(pageNumber);
			String[] record = p.records.get(recordNumber);
			resultList.add(record);
			String entry = String.format("Row: %s was selected from the table at time: %d", Arrays.toString(record), System.currentTimeMillis());
			t.traceLog.add(entry);
			FileManager.storeTable(tableName, t);
		}
		
		return resultList;
	}
	
	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		//i'm lazy
		return new ArrayList<String[]>();
	}
	
	public static String getFullTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		if (t == null) return null;
		else return t.traceLog.toString();	
	}
	
	public static String getLastTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		if (t == null) return null;
		else return t.traceLog.get(t.traceLog.size()-1);
	}
	
	
	public static void main(String []args) throws IOException
	{
		String[] cols = {"id","name","major","semester","gpa"};
		createTable("student", cols); String[] r1 = {"1", "stud1", "CS", "5", "0.9"}; insert("student", r1);
		String[] r2 = {"2", "stud2", "BI", "7", "1.2"}; 
		insert("student", r2);
		String[] r3 = {"3", "stud3", "CS", "2", "2.4"}; 
		insert("student", r3);
		String[] r4 = {"4", "stud4", "DMET", "9", "1.2"}; 
		insert("student", r4);
		String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
		insert("student", r5);
	}
	
	
	
}
