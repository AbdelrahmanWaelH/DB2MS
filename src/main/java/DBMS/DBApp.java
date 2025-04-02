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
		Table newTable = new Table(tableName,columnsNames);
		System.out.println(FileManager.storeTable(tableName, newTable));

	}
	
	public static void insert(String tableName, String[] record)
	{
		Table currentTable = FileManager.loadTable(tableName);

		if (currentTable.canInsertIntoLastPage(dataPageSize)){
			currentTable.insertRecord(tableName,record);
		}else{
			currentTable.addPage();
			currentTable.insertRecord(tableName,record);
		}


		FileManager.storeTable(tableName, currentTable);
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
		tmp.add(currentTable.getRecord(tableName, pageNumber,recordNumber));
		return tmp;
	}
	
	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table currentTable = FileManager.loadTable(tableName);
		return currentTable.selectWhere(cols,vals);
	}
	
	public static String getFullTrace(String tableName)
	{

		return FileManager.loadTable(tableName).getFullTrace(dataPageSize);
	}
	
	public static String getLastTrace(String tableName)
	{
		
		return FileManager.loadTable(tableName).getLastTrace();
	}
	
	
	public static void main(String []args) throws IOException
	{
		String[] tmp = {"col1", "col2"};
		 createTable("myTable", tmp);
		System.out.println("Hello");
	}
	
	
	
}
