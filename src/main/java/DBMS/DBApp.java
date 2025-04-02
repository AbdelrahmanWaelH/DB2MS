package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


public class DBApp
{
	static int dataPageSize = 10;
	
	public static void createTable(String tableName, String[] columnsNames)
	{
		Table newTable = new Table(columnsNames);
		System.out.println(FileManager.storeTable(tableName, newTable));

	}
	
	public static void insert(String tableName, String[] record)
	{
		Table currentTable = FileManager.loadTable(tableName);

		if (currentTable.canInsertIntoLastPage(dataPageSize)){
			currentTable.insertRecord(record);
		}else{
			currentTable.addPage();
			currentTable.insertRecord(record);
		}
		FileManager.storeTablePage(tableName,currentTable.getLastPageNumber(), currentTable.getLastPage());


		//currentTable.pages.add()
	}
	
	public static ArrayList<String []> select(String tableName)
	{
		Table currentTable = FileManager.loadTable(tableName);

		return currentTable.getAllTableContent();
	}
	
	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Table currentTable = FileManager.loadTable(tableName);
		ArrayList<String[]> tmp =  new ArrayList<String[]>();
		tmp.add(currentTable.getRecord(pageNumber,recordNumber));
		return tmp;
	}
	
	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table currentTable = FileManager.loadTable(tableName);
		return currentTable.selectWhere(cols,vals);
	}
	
	public static String getFullTrace(String tableName)
	{
		
		return "";
	}
	
	public static String getLastTrace(String tableName)
	{
		
		return "";
	}
	
	
	public static void main(String []args) throws IOException
	{
		String[] tmp = {"col1", "col2"};
		 createTable("myTable", tmp);
		System.out.println("Hello");
	}
	
	
	
}
