package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests17
{

	@Test(timeout = 1000000)
	public void test169TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sqf3a", cols0);
		String [][] records_sqf3a = new String[322][cols0.length];
		for(int i=0;i<322;i++)
		{
			records_sqf3a[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sqf3a[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sqf3a", records_sqf3a[i]);
		}
	
		int pageCount0 = (int)Math.ceil(322.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("sqf3a");
	
		String[] ConditionColumns01 = {"j"};
		String[] ConditionColumnsValues01 = {"j1"};
		DBApp.select("sqf3a", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("sqf3a");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((322%DBApp.dataPageSize))-1;
		DBApp.select("sqf3a", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"c","h","d","k"};
		String[] ConditionColumnsValues04 = {"c2","h7","d3","k7"};
		DBApp.select("sqf3a", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("sqf3a");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((322%DBApp.dataPageSize))-1;
		DBApp.select("sqf3a", pageSelectPointer06,recordSelectPointer06);
	
		String selectTrace0 = DBApp.getFullTrace("sqf3a");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 331);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 322"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("wi", cols1);
		String [][] records_wi = new String[8][cols1.length];
		for(int i=0;i<8;i++)
		{
			records_wi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wi", records_wi[i]);
		}
	
		int pageCount1 = (int)Math.ceil(8.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("wi");
	
		String[] ConditionColumns11 = {"c"};
		String[] ConditionColumnsValues11 = {"c0"};
		DBApp.select("wi", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("wi");
	
		DBApp.select("wi");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("wi", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("wi", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("wi", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("wi", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("wi", pageSelectPointer18,recordSelectPointer18);
	
		String selectTrace1 = DBApp.getFullTrace("wi");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 19);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 8"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("u78m7", cols2);
		String [][] records_u78m7 = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			records_u78m7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u78m7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u78m7", records_u78m7[i]);
		}
	
		int pageCount2 = (int)Math.ceil(22.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("u78m7", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("u78m7", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("u78m7");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("u78m7", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"c"};
		String[] ConditionColumnsValues24 = {"c2"};
		DBApp.select("u78m7", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("u78m7");
	
		DBApp.select("u78m7");
	
		String[] ConditionColumns27 = {"d","b"};
		String[] ConditionColumnsValues27 = {"d1","b1"};
		DBApp.select("u78m7", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("u78m7");
	
		DBApp.select("u78m7");
	
		String[] ConditionColumns210 = {"b"};
		String[] ConditionColumnsValues210 = {"b1"};
		DBApp.select("u78m7", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("u78m7", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("u78m7", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("u78m7", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"c"};
		String[] ConditionColumnsValues214 = {"c0"};
		DBApp.select("u78m7", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"a"};
		String[] ConditionColumnsValues215 = {"a5"};
		DBApp.select("u78m7", ConditionColumns215, ConditionColumnsValues215);
	
		String selectTrace2 = DBApp.getFullTrace("u78m7");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 40);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 22"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(662);
		return (int) (random.nextDouble()*max) + 1;
	}
	private static String genRandString() 
	{
		char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		char[] digits = "0123456789".toCharArray();
		int nameSize = genRandNum(5);
		if (nameSize < 2)
			nameSize += 2;
		String res = "";
		res += alphabet[(genRandNum(alphabet.length)) - 1];
		for (int i = 1; i < nameSize; i++)
			if (Math.random() >= 0.5)
				res += alphabet[(genRandNum(alphabet.length)) - 1];
			else
				res += digits[(genRandNum(digits.length)) - 1];
		return res;
	}
}