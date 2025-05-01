package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;


public class DBApp {
    static int dataPageSize = 10;

    public static void createTable(String tableName, String[] columnsNames) {
        Table newTable = new Table(tableName, columnsNames);
        System.out.println(FileManager.storeTable(tableName, newTable));

    }

    public static void insert(String tableName, String[] record) {
        Table currentTable = FileManager.loadTable(tableName);

        if (currentTable.canInsertIntoLastPage(dataPageSize)) {
            currentTable.insertRecord(tableName, record);
        } else {
            currentTable.addPage();
            currentTable.insertRecord(tableName, record);
        }
        for (int i = 0; i < currentTable.columns.length; i++) {
            if (currentTable.indices[i]) {
                BitmapIndex tmp = FileManager.loadTableIndex(currentTable.tableName, currentTable.columns[i]);
                tmp.updateIndex(currentTable, currentTable.columns[i], record, i);
            }
        }
        FileManager.storeTable(tableName, currentTable);
    }

    public static ArrayList<String[]> select(String tableName) {
        Table currentTable = FileManager.loadTable(tableName);

        return currentTable.getAllTableContent();
    }

    public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
        Table currentTable = FileManager.loadTable(tableName);
        ArrayList<String[]> tmp = new ArrayList<String[]>();
        tmp.add(currentTable.getRecord(tableName, pageNumber, recordNumber));
        return tmp;
    }

    public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
        Table currentTable = FileManager.loadTable(tableName);
        return currentTable.selectWhere(cols, vals);
    }

    public static String getFullTrace(String tableName) {

        return FileManager.loadTable(tableName).getFullTrace(dataPageSize);
    }

    public static String getLastTrace(String tableName) {

        return FileManager.loadTable(tableName).getLastTrace();
    }

    public static ArrayList<String[]> validateRecords(String tableName) {
        return null;
    }

	public static String toBinaryString(BitSet bs) {
		StringBuilder sb = new StringBuilder(bs.length());
		for (int i = 0; i < bs.length(); i++) {
			sb.append(bs.get(i) ? '1' : '0');
		}
		return sb.toString();
	}
    public static String getValueBits(String tableName, String colName, String value) {
        BitSet currBitSet = FileManager.loadTableIndex(tableName, colName).getBitset(value);
		return toBinaryString(currBitSet);
	}

    public static void createBitMapIndex(String tableName, String colName) {
        BitmapIndex newIndex = new BitmapIndex(tableName, colName);
        FileManager.storeTableIndex(tableName, colName, newIndex);
    }

    public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
        Table currentTable = FileManager.loadTable(tableName);
        ArrayList<BitSet> bitSets = new ArrayList<>();

        // this hashmap stores the position of the col in the table along with the string value it should be compared to
        HashMap<Integer, String> tableColNumTOValsString = new HashMap<>();
        for (int j = 0; j < cols.length; j++) {
            for (int i = 0; i < currentTable.columnNumber; i++) {
                if (currentTable.columns[i].equals(cols[j])) {
                    tableColNumTOValsString.put(i, vals[j]);
                }
            }
        }

        // this flag represents whether all columns filtered by are indexed with or not
        boolean flag = true;
        for (Map.Entry<Integer, String> entry : tableColNumTOValsString.entrySet()) {
            int key = entry.getKey();
            String value = entry.getValue();
            if (currentTable.indices[key]) {
                BitmapIndex tmp = FileManager.loadTableIndex(currentTable.tableName, currentTable.columns[key]);
                bitSets.add(tmp.getBitset(value));
            } else {
                flag = false;
            }
        }

        // ANDing (joining) the bitsets on a single bitset
        BitSet res = new BitSet();
        if (!bitSets.isEmpty()) {
            res = bitSets.get(0);
            for (BitSet tmp :
                    bitSets) {
                res.and(tmp);
            }
        }

        // if no indices then making that bitset to be all to basically be linear search
        if (res.cardinality() == 0) {
            int i = currentTable.getRecordsCount();
            res.set(0, i);
        }
        ArrayList<String[]> ans = new ArrayList<>();
        ArrayList<String[]> allRows = currentTable.getAllTableContent();


        // if no indices at all then can linearly search over all records in a single for loop
        // if all are indices then we will loop as well but only on set bits from the resulting bitset
        if (res.isEmpty() || !flag) {
            for (int idx = res.nextSetBit(0); idx >= 0; idx = res.nextSetBit(idx + 1)) {
                ans.add(allRows.get(idx));
            }
        } else {
            // in case we have some indexed and some not then we need to loop only on the set bits and then manually check the other columns
            allRecordLoop:
            for (int idx = res.nextSetBit(0); idx >= 0; idx = res.nextSetBit(idx + 1)) {
                String[] currRecord = allRows.get(idx);
                for (Map.Entry<Integer, String> entry : tableColNumTOValsString.entrySet()) {
                    int key = entry.getKey();
                    String value = entry.getValue();
                    if (!currRecord[key].equals(value)) {
                        continue allRecordLoop;
                    }
                }
                ans.add(currRecord);
            }
        }
        return ans;
    }


    public static void main(String[] args) throws IOException {
        FileManager.reset();


        String[] cols = {"id", "name", "major", "semester", "gpa"};
        createTable("student", cols);
        String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
        insert("student", r1);
        String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
        insert("student", r2);
        String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
        insert("student", r3);
        createBitMapIndex("student", "gpa");
        createBitMapIndex("student", "major");
        System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS"));
        System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));
        String[] r4 = {"4", "stud4", "CS", "9", "1.2"};
        insert("student", r4);
        String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
        insert("student", r5);
        System.out.println("After new insertions:");
        System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS"));
        System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));
        System.out.println("Output of selection using index when all columns of the select conditions are indexed:");
        ArrayList<String[]> result1 = selectIndex("student", new String[]{"major", "gpa"}, new String[]{"CS", "1.2"});
        for (String[] array : result1) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("Last trace of the table: " + getLastTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("Output of selection using index when only one column of the columns of the select conditions are indexed:");
        ArrayList<String[]> result2 = selectIndex("student", new String[]{"major", "semester"}, new String[]{"CS", "5"});
        for (String[] array : result2) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("Last trace of the table: " + getLastTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("Output of selection using index when some of the columns of the select conditions are indexed:");
        ArrayList<String[]> result3 = selectIndex("student", new String[]{"major", "semester", "gpa"}, new String[]{"CS", "5", "0.9"});
        for (String[] array : result3) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("Last trace of the table: " + getLastTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("Full Trace of the table:");
        System.out.println(getFullTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("The trace of the Tables Folder:");
        System.out.println(FileManager.trace());
    }


}
