package DBMS;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
public class Page implements Serializable
{
    ArrayList<String []> rows;

    public Page(){
        rows = new ArrayList<>();
    }
    public int getNumOfRecords() {
        if(rows != null)
            return rows.size();
        return 0;
    }
    public void insertRow(String[] row){
        if (rows == null){
            rows = new ArrayList<String []>();
        }
        rows.add(row);
    }
    public ArrayList<String[]> getRows(){
        return rows;
    }
    public String[] getRecord(int recordNumber){
        return rows.get(recordNumber);
    }
}
