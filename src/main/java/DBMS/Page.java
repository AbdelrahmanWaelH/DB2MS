package DBMS;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
public class Page implements Serializable
{
    ArrayList<ArrayList<String>> rows;


    public int getNumOfRecords() {
        if(rows != null)
            return rows.size();
        return 0;
    }
    public void insertRow(String[] row){
        if (rows == null){
            rows = new ArrayList<ArrayList<String>>();
        }
        rows.add(new ArrayList<>(Arrays.asList(row)));
    }
}
