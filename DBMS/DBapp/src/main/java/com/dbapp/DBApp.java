package com.dbapp;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.dbapp.FileManager.*;


public class DBApp {

	// 25 rows per each page
	static int dataPageSize = 2;

	// helper function to add the traces to a table
	public static void addTracesToTable(String tableName, StringBuilder builder){
		Table t = loadTable(tableName);
		t.getTraces().add(builder.toString());
		storeTable(tableName, t);
	}

	public static List<BitmapIndex> getAllBitMapIndexForTable(String tableName){
		Table t = loadTable(tableName);
		List<BitmapIndex> indexes = new ArrayList<>();
		String[] columnNames = t.getColumnsNames();
		for(String columnName:columnNames){
			BitmapIndex indx = loadTableIndex(tableName,columnName);
			if(indx != null){
				indexes.add(indx);
			}
		}
		return indexes;
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

		// check the index and insert it into the index if any
		List<BitmapIndex> indexes = getAllBitMapIndexForTable(tableName);
		int pageId = 0;
		int recordCnt = 0;
		ArrayList<String[]> rows = new ArrayList<>();
		Page pg = loadTablePage(tableName, pageId);

		while (pg != null) {
			List<String []> rs = pg.getRows();
			rows.addAll(rs);
			recordCnt += rs.size();
			pageId++;
			pg = loadTablePage(tableName, pageId);
		}

		for(BitmapIndex indx:indexes){
			Map<String, String> index = indx.getIndex();
			int columnIndex = indx.getColumnIndex();

			// Determine the size of the bit string for previous records
			int size = Math.max(0, recordCnt - 1);

			// If the value is not yet in the index, initialize it with 0s and a 1 at the end
			if (!index.containsKey(record[columnIndex])) {
				index.put(record[columnIndex], "0".repeat(size) + "1");
			}

			// For all keys in the index, append 1 if it matches the value, else 0
			for (String k : index.keySet()) {
				if (!k.equals(record[columnIndex])) {
					index.put(k, index.get(k) + "0");
				} else if (index.containsKey(k) && !index.get(k).endsWith("1")) {
					// Only append "1" if we didn't just add it above (i.e., already existed)
					index.put(k, index.get(k) + "1");
				}
			}

			// update the index with the corresponding new index
			indx.setIndex(index);

			// store the newly updated index as file
			storeTableIndex(tableName,indx.getColumnName(),indx);
		}
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
		Map<Integer, Integer> sortedMap = new TreeMap<>(recordsPerPage); // ensures keys are sorted

		StringBuilder resultBuilder = new StringBuilder();
		resultBuilder.append("[");
		Iterator<Map.Entry<Integer, Integer>> iterator = sortedMap.entrySet().iterator();
		while (iterator.hasNext()) {
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
		List<BitmapIndex> indexes = getAllBitMapIndexForTable(tableName);
		List<String> indexedCols = indexes.stream().map(BitmapIndex::getColumnName).collect(Collectors.toList());
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
		builder.append(", ");
		builder.append("Indexed Columns: ");
		builder.append(Arrays.toString(indexedCols.toArray(new String[0])));
		return builder.toString();
	}
	
	public static String getLastTrace(String tableName) {
		Table t = loadTable(tableName);
		List<String> traces = t.getTraces();
		return t.getTraces().get(traces.size()-1);
	}

	public static ArrayList<String []> validateRecords(String tableName){
		return new ArrayList<>();
	}

	public static void recoverRecords(String tableName, ArrayList<String[]> missing){

	}

	public static void createBitMapIndex(String tableName, String colName){
		long startTime = System.currentTimeMillis();
		// load all the data for a given table
		Table t = loadTable(tableName);
		int pageIndex = 0;
		ArrayList<String[]> rows = new ArrayList<>();
		Page p = loadTablePage(tableName, pageIndex);

		while (p != null) {
			List<String []> rs = p.getRows();
			rows.addAll(rs);
			pageIndex++;
			p = loadTablePage(tableName, pageIndex);
		}
		// find the colName position
		String[] colNames = t.getColumnsNames();
		Integer columnIndex = null;
		for(int i=0;i<colNames.length;i++){
			if(colNames[i].equalsIgnoreCase(colName)){
				columnIndex = i;
			}
		}
		if(columnIndex == null){
			throw new RuntimeException("Column not found");
		}
		// filter out the unique values in that column
		Set<String> uniqueValues = new HashSet<>();
		int finalColumnIndex = columnIndex;

		//collect all unique values from the specified column
		for (String[] row : rows) {
			uniqueValues.add(row[finalColumnIndex]);
		}

		//initialize the map with binary strings of 0s
		Map<String, StringBuilder> index = new HashMap<>();
		for (String value : uniqueValues) {
			StringBuilder binary = new StringBuilder("0".repeat(rows.size()));
			index.put(value, binary);
		}

		//set the appropriate bit to 1 for each row
		for (int j = 0; j < rows.size(); j++) {
			String[] row = rows.get(j);
			String value = row[finalColumnIndex];
			index.get(value).setCharAt(j, '1');
		}

		//convert to String map if needed
		Map<String, String> finalIndex = new HashMap<>();
		for (Map.Entry<String, StringBuilder> entry : index.entrySet()) {
			finalIndex.put(entry.getKey(), entry.getValue().toString());
		}
		BitmapIndex bIndx = BitmapIndex
				.builder()
				.index(finalIndex)
				.columnName(colName)
				.columnIndex(finalColumnIndex)
				.tableName(tableName)
				.build();

		storeTableIndex(tableName,colName,bIndx);

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		StringBuilder builder = new StringBuilder();
		builder.append("Index created for column: ");
		builder.append(colName);
		builder.append(", ");
		builder.append("execution time (mil):");
		builder.append(executionTime);
		addTracesToTable(tableName,builder);
	}

	public static String getValueBits(String tableName, String colName, String value) {
		BitmapIndex in = loadTableIndex(tableName, colName);
		String bits = in.getIndex().get(value);

		if(bits!=null)
			return bits;

		int pageId = 0;
		int recordCnt = 0;

		Page pg = loadTablePage(tableName, pageId);
		while (pg != null) {
			recordCnt += pg.getRows().size();
			pageId++;
			pg = loadTablePage(tableName, pageId);
		}

		return "0".repeat(recordCnt);
	}

	public static ArrayList<String []> selectIndex(String tableName, String[] cols, String[] vals) {
		List<BitmapIndex> indexes = getAllBitMapIndexForTable(tableName);
		Set<String> indexedCols = indexes.stream().map(BitmapIndex::getColumnName).collect(Collectors.toSet());
		Map<String, BitmapIndex> columnNameIndex = indexes.stream().collect(Collectors.toMap(BitmapIndex::getColumnName, Function.identity()));
		if (indexedCols.containsAll(Arrays.asList(cols))) {
			// all the columns are indexed just use bitwise AND
			long startTime = System.currentTimeMillis();
			int size = indexes.get(0).getIndex().values().iterator().next().length();
			String finalMatch = "1".repeat(size);
			for(int i=0;i<cols.length;i++) {
				String colName = cols[i];
				BitmapIndex indx = columnNameIndex.get(colName);
				String valueBits = indx.getIndex().get(vals[i]);
				// Bitwise AND
				StringBuilder result = new StringBuilder(size);
				for (int j = 0; j < size; j++) {
					char bitA = finalMatch.charAt(j);
					char bitB = valueBits.charAt(j);
					result.append((bitA == '1' && bitB == '1') ? '1' : '0');
				}

				finalMatch = result.toString();
			}
			ArrayList<String[]> matches = new ArrayList<>();
			for (int i = 0; i < finalMatch.length(); i++) {
				if (finalMatch.charAt(i) == '1') {
					int pageNumber = i / dataPageSize;
					int rowNumber = i % dataPageSize;

					Page page = loadTablePage(tableName, pageNumber);
					List<String[]> rows = page.getRows();

					if (rowNumber < rows.size()) {
						matches.add(rows.get(rowNumber));
					}
				}
			}
			long endTime = System.currentTimeMillis();
			long executionTime = endTime - startTime;
			StringBuilder builder = new StringBuilder();
			builder.append("Select index condition: ");
			builder.append(Arrays.toString(cols));
			builder.append("->");
			builder.append(Arrays.toString(vals));
			builder.append(", ");
			builder.append("Indexed columns: ");
			builder.append(Arrays.toString(cols));
			builder.append(", ");
			builder.append("Indexed selection count: ");
			builder.append(matches.size());
			builder.append(", ");
			builder.append("Final count: ");
			builder.append(matches.size());
			builder.append(", ");
			builder.append("execution time (mil):");
			builder.append(executionTime);
			addTracesToTable(tableName, builder);
			return matches;
		}
		else if (Collections.disjoint(indexedCols, Arrays.asList(cols))) {
			// No Indexed at all , use the normal select
			long startTime = System.currentTimeMillis();
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
			try {
				for (Map.Entry<Integer, List<String[]>> e : pageRowsMap.entrySet()) {
					List<String[]> rows = e.getValue();
					for (String[] r : rows) {
						boolean hasDiff = false;
						for (int k = 0; k < columnIndexes.size(); k++) {
							if (!r[columnIndexes.get(k)].equals(vals[k])) {
								hasDiff = true;
								break;
							}
						}
						if (!hasDiff) {
							matches.add(r);
						}
					}
				}
			} catch (Exception ignored) {
			}
			long endTime = System.currentTimeMillis();
			long executionTime = endTime - startTime;
			StringBuilder builder = new StringBuilder();
			builder.append("Select index condition: ");
			builder.append(Arrays.toString(cols));
			builder.append("->");
			builder.append(Arrays.toString(vals));
			builder.append(", ");
			builder.append("Indexed selection count: 0");
			builder.append(", ");
			builder.append("Non Indexed: ");
			builder.append(Arrays.toString(cols));
			builder.append(", ");
			builder.append("Final count: ");
			builder.append(matches.size());
			builder.append(", ");
			builder.append("execution time (mil):");
			builder.append(executionTime);
			addTracesToTable(tableName, builder);
			return matches;
		}
		else{
			// partial Index , some columns have index and some does not
			long startTime = System.currentTimeMillis();
			List<String> indexedColumns = new ArrayList<>();
			List<String> indexedValues = new ArrayList<>();
			List<String> nonIndexedColumns = new ArrayList<>();
			List<String> nonIndexedValues = new ArrayList<>();

			// Find the matches using the indexed columns
			ArrayList<String[]> indexedMatches = new ArrayList<>();
			for(int i=0;i<cols.length;i++){
				if(indexedCols.contains(cols[i])){
					indexedColumns.add(cols[i]);
					indexedValues.add(vals[i]);
				}else{
					nonIndexedColumns.add(cols[i]);
					nonIndexedValues.add(vals[i]);
				}
			}
			int size = indexes.get(0).getIndex().values().iterator().next().length();
			String finalMatch = "1".repeat(size);
			for(int i=0;i<indexedColumns.size();i++) {
				String colName = indexedColumns.get(i);
				BitmapIndex indx = columnNameIndex.get(colName);
				String valueBits = indx.getIndex().get(indexedValues.get(i));
				// Bitwise AND
				StringBuilder result = new StringBuilder(size);
				for (int j = 0; j < size; j++) {
					char bitA = finalMatch.charAt(j);
					char bitB = valueBits.charAt(j);
					result.append((bitA == '1' && bitB == '1') ? '1' : '0');
				}

				finalMatch = result.toString();
			}
			for (int i = 0; i < finalMatch.length(); i++) {
				if (finalMatch.charAt(i) == '1') {
					int pageNumber = i / dataPageSize;
					int rowNumber = i % dataPageSize;

					Page page = loadTablePage(tableName, pageNumber);
					List<String[]> rows = page.getRows();

					if (rowNumber < rows.size()) {
						indexedMatches.add(rows.get(rowNumber));
					}
				}
			}

			ArrayList<String[]> linearMatches = new ArrayList<>();
			int pageIndex = 0;
			Map<Integer, List<String[]>> pageRowsMap = new HashMap<>();
			Page p = loadTablePage(tableName, pageIndex);
			while (p != null) {
				List<String[]> rs = p.getRows();
				pageRowsMap.put(pageIndex, rs); // Store the rows for the current page
				pageIndex++;
				p = loadTablePage(tableName, pageIndex); // Move to the next page
			}

			// load the table to get the column names
			Table t = loadTable(tableName);
			String[] columnNames = t.getColumnsNames();
			// get the index of each column to do the filter
			List<Integer> columnIndexes = new ArrayList<>();
			for (int j = 0; j < nonIndexedColumns.size(); j++) {
				for (int i = 0; i < columnNames.length; i++) {
					if (columnNames[i].equalsIgnoreCase(nonIndexedColumns.get(j))) {
						columnIndexes.add(i);
						break;
					}
				}
			}
			// filter the rows satisfying the query
			try {
				for (Map.Entry<Integer, List<String[]>> e : pageRowsMap.entrySet()) {
					List<String[]> rows = e.getValue();
					for (String[] r : rows) {
						boolean hasDiff = false;
						for (int k = 0; k < columnIndexes.size(); k++) {
							if (!r[columnIndexes.get(k)].equals(nonIndexedValues.get(k))) {
								hasDiff = true;
								break;
							}
						}
						if (!hasDiff) {
							linearMatches.add(r);
						}
					}
				}
			} catch (Exception ignored) {
			}

			// get the intersection between both of the results
			List<String[]> matches = indexedMatches.stream()
					.filter(indexedArr ->
							linearMatches.stream().anyMatch(linearArr -> Arrays.equals(indexedArr, linearArr))
					)
					.collect(Collectors.toList());

			long endTime = System.currentTimeMillis();
			long executionTime = endTime - startTime;
			StringBuilder builder = new StringBuilder();
			builder.append("Select index condition: ");
			builder.append(Arrays.toString(cols));
			builder.append("->");
			builder.append(Arrays.toString(vals));
			builder.append(", ");
			builder.append("Indexed columns:");
			builder.append(Arrays.toString(indexedColumns.toArray(new String[0])));
			builder.append(", ");
			builder.append("Indexed selection count: ");
			builder.append(indexedMatches.size());
			builder.append(", ");
			builder.append("Non Indexed: ");
			builder.append(Arrays.toString(nonIndexedColumns.toArray(new String[0])));
			builder.append(", ");
			builder.append("Final count: ");
			builder.append(matches.size());
			builder.append(", ");
			builder.append("execution time (mil):");
			builder.append(executionTime);
			addTracesToTable(tableName, builder);
			return new ArrayList<>(matches);
		}
	}

	public static void main(String []args) throws IOException
	{
		FileManager.reset();
		String[] cols = {"id","name","major","semester","gpa"};
		createTable("student", cols);
		String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
		insert("student", r1);
		String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
		insert("student", r2);
		String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
		insert("student", r3);
		createBitMapIndex("student", "gpa");
		createBitMapIndex("student", "major");
		System.out.println("Bitmap of the value of CS from the major index:" +getValueBits("student", "major", "CS"));
		System.out.println("Bitmap of the value of 1.2 from the gpa index: "+getValueBits("student", "gpa", "1.2"));
		String[] r4 = {"4", "stud4", "CS", "9", "1.2"};
		insert("student", r4);
		String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
		insert("student", r5);
		System.out.println("After new insertions:");
		System.out.println("Bitmap of the value of CS from the major index: "+getValueBits("student", "major", "CS"));
				System.out.println("Bitmap of the value of 1.2 from the gpa index: "+getValueBits("student", "gpa", "1.2"));
		System.out.println("Output of selection using index when all columns of the select conditions are indexed:");
		ArrayList<String[]> result1 = selectIndex("student", new String[]
				{"major","gpa"}, new String[] {"CS","1.2"});
		for (String[] array : result1) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("Last trace of the table: "+getLastTrace("student"));
		System.out.println("--------------------------------");

		System.out.println("Output of selection using index when only one column of the columns of the select conditions are indexed:");
		ArrayList<String[]> result2 = selectIndex("student", new String[]
				{"major","semester"}, new String[] {"CS","5"});
		for (String[] array : result2) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("Last trace of the table: "+getLastTrace("student"));
		System.out.println("--------------------------------");

		System.out.println("Output of selection using index when some of the columns of the select conditions are indexed:");
		ArrayList<String[]> result3 = selectIndex("student", new String[]
				{"major","semester","gpa" }, new String[] {"CS","5", "0.9"});
		for (String[] array : result3) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("Last trace of the table: "+getLastTrace("student"));
		System.out.println("--------------------------------");

		System.out.println("Full Trace of the table:");
		System.out.println(getFullTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder:");
		System.out.println(FileManager.trace());
	}

}
