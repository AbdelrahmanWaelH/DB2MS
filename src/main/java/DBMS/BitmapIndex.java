package DBMS;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

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


public class BitmapIndex {
    String tableName;
    String colname;

    HashMap<String, BitSet> bitsets;

    public  BitmapIndex(String tableName, String colName){
        this.tableName = tableName;
        this.colname = colName;
        Table currTable = FileManager.loadTable(tableName);
        ArrayList<String[]> allRows = currTable.getAllTableContent();

        bitsets = new HashMap<>();
        int colNumber=0;

        //getting the column number
        for( int i=0; i< currTable.columns.length; i++  ) {
            if (currTable.columns[i].equals(colname))
                colNumber = i;
        }
        currTable.indices[colNumber] = true;

        // creating the index as a hashmap of bits
        for (int i=0; i< allRows.size(); i++) {
            BitSet currValue = bitsets.getOrDefault(allRows.get(colNumber)[i],null);
            // creating a bitset in case of a new value
            if(currValue == null){
                bitsets.put(allRows.get(colNumber)[i], new BitSet(i));
            }
            // setting the corresponding bit in all cases
            bitsets.get(allRows.get(colNumber)[i]).set(i);
        }
//        FileManager.storeTableIndex(tableName, colName, this);
    }

    void updateIndex(Table table, String colName, String[] record, int i){
        BitSet currValue =bitsets.getOrDefault(record[i], null);
        if(currValue == null){
            bitsets.put(record[i], new BitSet(i));
        }
        // setting the corresponding bit in all cases
        bitsets.get(record[i]).set(i);

        FileManager.storeTableIndex(table.tableName, colName, this);
    }

}


/*for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx+1)) {
        System.out.println("Bit set at index: " + idx);
        }

*/