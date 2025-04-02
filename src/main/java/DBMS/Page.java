package DBMS;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
public class Page implements Serializable
{
    ArrayList<ArrayList<String>> rows;


    public int getNumOfRecords() {
        return rows.size();
    }
    public void insertRow(String[] row){
        rows.add(new ArrayList<>(Arrays.asList(row)));
    }
}
