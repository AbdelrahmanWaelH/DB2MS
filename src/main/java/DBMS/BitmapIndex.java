package DBMS;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/*
    create a bitset for every value in the table column
    append to all bitsets whenever a new value is added on to the table

*/

/**
 * bitset usage:
 * Method	                What it does
 * bits.set(int index)	    Sets bit at index to true
 * bits.clear(int index)	Sets bit at index to false
 * bits.get(int index)	    Returns boolean value of the bit at index
 * bits.flip(int index)	    Toggles the bit at index
 * bits.cardinality()	    Returns number of bits set to true
 * bits.length()        	Highest set bit index + 1
 * bits.size()
 */


public class BitmapIndex implements Serializable {
    String tableName;
    String colname;

    HashMap<String, BitSet> bitsets;
    int totalRowsInTable;

    public  BitmapIndex(String tableName, String colName){
        this.tableName = tableName;
        this.colname = colName;
        Table currTable = FileManager.loadTable(tableName);
        this.totalRowsInTable =  currTable.recordsCount;
        ArrayList<String[]> allRows = currTable.getAllTableContent();

        bitsets = new HashMap<>();
        int colNumber=0;

        //getting the column number
        for( int i=0; i< currTable.columns.length; i++  ) {
            if (currTable.columns[i].equals(colname))
                colNumber = i;
        }
        currTable.indices[colNumber] = true;
        FileManager.storeTable(currTable.tableName, currTable);
        // creating the index as a hashmap of bits
        for (int i=0; i< allRows.size(); i++) {
            BitSet currValue = bitsets.getOrDefault(allRows.get(i)[colNumber],null);
            // creating a bitset in case of a new value
            if(currValue == null){
                bitsets.put(allRows.get(i)[colNumber], new BitSet(i));
            }
            // setting the corresponding bit in all cases
            bitsets.get(allRows.get(i)[colNumber]).set(i);
        }
//        FileManager.storeTableIndex(tableName, colName, this);
    }

    public BitSet getBitset(String value){
        return bitsets.get(value);
    }
    void updateIndex(Table table, String colName, String[] record, int i){
        BitSet currValue =bitsets.getOrDefault(record[i], null);
        if(currValue == null){
            bitsets.put(record[i], new BitSet(totalRowsInTable));
        }
        // setting the corresponding bit in all cases
        bitsets.get(record[i]).set(totalRowsInTable);
        this.totalRowsInTable =  table.recordsCount;
        //this.printIndex();
        FileManager.storeTableIndex(table.tableName, colName, this);
    }

    public BitSet getColumnValueBits(String value){
        return bitsets.get(value);
    }

    public static String toBinaryString(BitSet bs, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(bs.get(i) ? '1' : '0');
        }
        return sb.toString();
    }
    public void printIndex() {
        System.out.println("Index on table '" + tableName + "', column '" + colname + "':");
        for (Map.Entry<String, BitSet> e : bitsets.entrySet()) {
            String value = e.getKey();
            BitSet bits = e.getValue();
            System.out.printf("  %-10s → %s%n", value, toBinaryString(bits, this.totalRowsInTable));
        }
    }

}


/*for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx+1)) {
        System.out.println("Bit set at index: " + idx);
        }

*/