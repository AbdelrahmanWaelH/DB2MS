package com.dbapp;

import java.io.IOException;
import java.util.*;

import static com.dbapp.FileManager.*;


public class DBApp {

	// 25 rows per each page
	static int dataPageSize = 200;

	// helper function to add the traces to a table
	public static void addTracesToTable(String tableName, StringBuilder builder){
		Table t = loadTable(tableName);
		t.getTraces().add(builder.toString());
		storeTable(tableName, t);
	}

	public static void createTable(String tableName, String[] columnsNames) {
		StringBuilder builder = new StringBuilder();
		builder.append("Table created name:");
		builder.append(tableName);
		builder.append(", ");
		builder.append("columnsNames:");
		builder.append(Arrays.toString(columnsNames));

		List<String> traces = new ArrayList<>();
		traces.add(builder.toString());

		Table t = Table.builder()
				.columnsNames(columnsNames)
				.traces(traces)
				.build();

		storeTable(tableName,t);
	}

	public static void insert(String tableName, String[] record) {
		// record the start time for the trace
		long startTime = System.currentTimeMillis();

		int pageIndex = 0;
		Page p = loadTablePage(tableName, pageIndex);

		while (p != null && p.isFull(dataPageSize)) {
			pageIndex++;
			p = loadTablePage(tableName, pageIndex);
		}

		if (p == null) {
			// No available page found, create a new one
			List<String[]> rows = new ArrayList<>();
			rows.add(record);
			p = new Page(rows);
		} else {
			// Found a non-full page, add the record
			p.getRows().add(record);
		}

		// Store the updated page
		storeTablePage(tableName, pageIndex, p);

		// build the trace
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		StringBuilder builder = new StringBuilder();
		builder.append("Inserted:");
		builder.append(Arrays.toString(record));
		builder.append(", ");
		builder.append("at page number:");
		builder.append(pageIndex);
		builder.append(", ");
		builder.append("execution time (mil):");
		builder.append(executionTime);

		// add the traces to the table and update the table
		addTracesToTable(tableName,builder);
	}

	// find the full data in a table
	public static ArrayList<String []> select(String tableName) {
		long startTime = System.currentTimeMillis();
		int pageIndex = 0;
		int recordCnt = 0;
		ArrayList<String[]> rows = new ArrayList<>();
		Page p = loadTablePage(tableName, pageIndex);

		while (p != null) {
			List<String []> rs = p.getRows();
			rows.addAll(rs);
			recordCnt += rs.size();
			pageIndex++;
			p = loadTablePage(tableName, pageIndex);
		}

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		StringBuilder builder = new StringBuilder();

		builder.append("Select all pages:");
		builder.append(pageIndex);
		builder.append(", ");
		builder.append("records:");
		builder.append(recordCnt);
		builder.append(", ");
		builder.append("execution time (mil):");
		builder.append(executionTime);

		addTracesToTable(tableName,builder);
		return rows;
	}
	
	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber) {
		long startTime = System.currentTimeMillis();
		ArrayList<String[]> rows = new ArrayList<>();
		Page p = loadTablePage(tableName, pageNumber);
		if(p!=null) {
			try{
				String [] arr = p.getRows().get(recordNumber);
				rows.add(arr);
			}catch(Exception ignored){}
		}
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		StringBuilder builder = new StringBuilder();
		builder.append("Select pointer page:");
		builder.append(pageNumber);
		builder.append(", ");
		builder.append("record:");
		builder.append(recordNumber);
		builder.append(", ");
		builder.append("total output count:");
		builder.append(rows.size());
		builder.append(", ");
		builder.append("execution time (mil):");
		builder.append(executionTime);

		addTracesToTable(tableName,builder);
		return rows;
	}
	
	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals) {
		long startTime = System.currentTimeMillis();
		// Find all the rows
		int pageIndex = 0;
		Map<Integer, List<String[]>> pageRowsMap = new HashMap<>();
		Page p = loadTablePage(tableName, pageIndex);
		while (p != null) {
			List<String[]> rs = p.getRows();
			pageRowsMap.put(pageIndex, rs); // Store the rows for the current page
			pageIndex++;
			p = loadTablePage(tableName, pageIndex); // Move to the next page
		}

		ArrayList<String[]> matches = new ArrayList<>();

		// load the table to get the column names
		Table t = loadTable(tableName);
		String[] columnNames = t.getColumnsNames();

		// get the index of each column to do the filter
		List<Integer> columnIndexes = new ArrayList<>();
		for (int j = 0; j < cols.length; j++) {
			for (int i = 0; i < columnNames.length; i++) {
				if (columnNames[i].equalsIgnoreCase(cols[j])) {
					columnIndexes.add(i);
					break;
				}
			}
		}
		// filter the rows satisfying the query
		Map<Integer,Integer> recordsPerPage = new HashMap<>();
		try{
			for(Map.Entry<Integer, List<String[]>> e : pageRowsMap.entrySet()) {
				List<String []> rows = e.getValue();
				int pageMatches = 0;
				for(String [] r : rows){
					boolean hasDiff = false;
					for(int k = 0; k < columnIndexes.size(); k++) {
						if(!r[columnIndexes.get(k)].equals(vals[k])) {
							hasDiff = true;
							break;
						}
					}
					if(!hasDiff) {
						matches.add(r);
						pageMatches++;
					}
				}
				if(pageMatches!=0){
					recordsPerPage.put(e.getKey(), pageMatches);
				}
			}
		} catch (Exception ignored) {}

		StringBuilder resultBuilder = new StringBuilder();
		resultBuilder.append("[");
		for (Iterator<Map.Entry<Integer, Integer>> iterator = recordsPerPage.entrySet().iterator(); iterator.hasNext(); ) {
			Map.Entry<Integer, Integer> entry = iterator.next();
			resultBuilder.append("[").append(entry.getKey()).append(", ").append(entry.getValue()).append("]");
			if (iterator.hasNext()) {
				resultBuilder.append(", ");
			}
		}
		resultBuilder.append("]");

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		StringBuilder builder = new StringBuilder();
		builder.append("Select condition:");
		builder.append(Arrays.toString(cols));
		builder.append("->");
		builder.append(Arrays.toString(vals));
		builder.append(", ");
		builder.append("Records per page:");
		builder.append(resultBuilder);
		builder.append(", ");
		builder.append("records:");
		builder.append(matches.size());
		builder.append(", ");
		builder.append("execution time (mil):");
		builder.append(executionTime);

		addTracesToTable(tableName,builder);
		return matches;
	}
	
	public static String getFullTrace(String tableName) {
		StringBuilder builder = new StringBuilder();
		Table t = loadTable(tableName);
		List<String> traces = t.getTraces();
		for(String trace : traces) {
			builder.append(trace);
			builder.append("\n");
		}

		int recordCnt = 0;
		int pageIndex = 0;
		ArrayList<String[]> rows = new ArrayList<>();
		Page p = loadTablePage(tableName, pageIndex);

		while (p != null) {
			List<String []> rs = p.getRows();
			rows.addAll(rs);
			recordCnt += rs.size();
			pageIndex++;
			p = loadTablePage(tableName, pageIndex);
		}

		int recordsCnt = rows.size();
		int pagesCount = (int) Math.ceil((double) recordsCnt / dataPageSize);
		builder.append("Pages Count: ");
		builder.append(pagesCount);
		builder.append(", ");
		builder.append("Records Count: ");
		builder.append(recordsCnt);

		return builder.toString();
	}
	
	public static String getLastTrace(String tableName) {
		Table t = loadTable(tableName);
		List<String> traces = t.getTraces();
		return t.getTraces().get(traces.size()-1);
	}


	public static void main(String[] args) throws IOException {
		String[] cols = {
				"id",
				"name",
				"major",
				"semester",
				"gpa"
		};
		createTable("student", cols);
		String[] r1 = {
				"1",
				"stud1",
				"CS",
				"5",
				"0.9"
		};
		insert("student", r1);
		String[] r2 = {
				"2",
				"stud2",
				"BI",
				"7",
				"1.2"
		};
		insert("student", r2);
		String[] r3 = {
				"3",
				"stud3",
				"CS",
				"2",
				"2.4"
		};
		insert("student", r3);
		String[] r4 = {
				"4",
				"stud4",
				"DMET",
				"9",
				"1.2"
		};
		insert("student", r4);
		String[] r5 = {
				"5",
				"stud5",
				"BI",
				"4",
				"3.5"
		};
		insert("student", r5);
		System.out.println("Output of selecting the whole table content:");
		ArrayList < String[] > result1 = select("student");
		for (String[] array: result1) {
			for (String str: array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}

		System.out.println("--------------------------------");
		System.out.println("Output of selecting the output by position:");
		ArrayList < String[] > result2 = select("student", 1, 1);
		for (String[] array: result2) {
			for (String str: array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}

		System.out.println("--------------------------------");
		System.out.println("Output of selecting the output by column condition:");
		ArrayList < String[] > result3 = select("student", new String[] {
				"gpa"
		}, new String[] {
				"1.2"
		});
		for (String[] array: result3) {
			for (String str: array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("--------------------------------");
		System.out.println("Full Trace of the table:");
		System.out.println(getFullTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("Last Trace of the table:");
		System.out.println(getLastTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder:");
		System.out.println(FileManager.trace());
		FileManager.reset();
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder after resetting:");
		System.out.println(FileManager.trace());
	}

}
