package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests16
{

	@Test(timeout = 1000000)
	public void test114TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ekj", cols0);
		String [][] records_ekj = new String[467][cols0.length];
		for(int i=0;i<467;i++)
		{
			records_ekj[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ekj[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ekj", records_ekj[i]);
		}
	
		int pageCount0 = (int)Math.ceil(467.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((467%DBApp.dataPageSize))-1;
		DBApp.select("ekj", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("ekj");
	
		DBApp.select("ekj");
	
		DBApp.select("ekj");
	
		DBApp.select("ekj");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((467%DBApp.dataPageSize))-1;
		DBApp.select("ekj", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("ekj");
	
		DBApp.select("ekj");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((467%DBApp.dataPageSize))-1;
		DBApp.select("ekj", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((467%DBApp.dataPageSize))-1;
		DBApp.select("ekj", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("ekj");
	
		String[] ConditionColumns011 = {"n","c","g","a","b","k","m"};
		String[] ConditionColumnsValues011 = {"n9","c1","g2","a37","b1","k4","m11"};
		DBApp.select("ekj", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"i","d","h"};
		String[] ConditionColumnsValues012 = {"i5","d3","h7"};
		DBApp.select("ekj", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"g","l","m","k","f"};
		String[] ConditionColumnsValues013 = {"g0","l9","m1","k6","f3"};
		DBApp.select("ekj", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("ekj");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((467%DBApp.dataPageSize))-1;
		DBApp.select("ekj", pageSelectPointer015,recordSelectPointer015);
	
		String selectTrace0 = DBApp.getFullTrace("ekj");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 485);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 467"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vf6", cols1);
		String [][] records_vf6 = new String[175][cols1.length];
		for(int i=0;i<175;i++)
		{
			records_vf6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vf6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vf6", records_vf6[i]);
		}
	
		int pageCount1 = (int)Math.ceil(175.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		DBApp.select("vf6");
	
		String[] ConditionColumns11 = {"h"};
		String[] ConditionColumnsValues11 = {"h7"};
		DBApp.select("vf6", ConditionColumns11, ConditionColumnsValues11);
	
		String selectTrace1 = DBApp.getFullTrace("vf6");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 179);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 175"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("u16", cols2);
		String [][] records_u16 = new String[119][cols2.length];
		for(int i=0;i<119;i++)
		{
			records_u16[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_u16[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("u16", records_u16[i]);
		}
	
		int pageCount2 = (int)Math.ceil(119.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("u16", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"e"};
		String[] ConditionColumnsValues21 = {"e3"};
		DBApp.select("u16", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("u16");
	
		DBApp.select("u16");
	
		String[] ConditionColumns24 = {"n"};
		String[] ConditionColumnsValues24 = {"n5"};
		DBApp.select("u16", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("u16", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"e"};
		String[] ConditionColumnsValues26 = {"e1"};
		DBApp.select("u16", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("u16");
	
		DBApp.select("u16");
	
		DBApp.select("u16");
	
		String[] ConditionColumns210 = {"i","a"};
		String[] ConditionColumnsValues210 = {"i1","a73"};
		DBApp.select("u16", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("u16", pageSelectPointer211,recordSelectPointer211);
	
		String[] ConditionColumns212 = {"b","c","l","k","g","a","e"};
		String[] ConditionColumnsValues212 = {"b0","c2","l2","k6","g1","a50","e0"};
		DBApp.select("u16", ConditionColumns212, ConditionColumnsValues212);
	
		String selectTrace2 = DBApp.getFullTrace("u16");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 134);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 119"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("b00x", cols3);
		String [][] records_b00x = new String[141][cols3.length];
		for(int i=0;i<141;i++)
		{
			records_b00x[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b00x[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b00x", records_b00x[i]);
		}
	
		int pageCount3 = (int)Math.ceil(141.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((141%DBApp.dataPageSize))-1;
		DBApp.select("b00x", pageSelectPointer30,recordSelectPointer30);
	
		DBApp.select("b00x");
	
		String[] ConditionColumns32 = {"j"};
		String[] ConditionColumnsValues32 = {"j8"};
		DBApp.select("b00x", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("b00x");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((141%DBApp.dataPageSize))-1;
		DBApp.select("b00x", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("b00x");
	
		String[] ConditionColumns36 = {"k"};
		String[] ConditionColumnsValues36 = {"k9"};
		DBApp.select("b00x", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("b00x");
	
		String[] ConditionColumns38 = {"d","l","b","h"};
		String[] ConditionColumnsValues38 = {"d2","l10","b0","h6"};
		DBApp.select("b00x", ConditionColumns38, ConditionColumnsValues38);
	
		DBApp.select("b00x");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((141%DBApp.dataPageSize))-1;
		DBApp.select("b00x", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("b00x");
	
		String selectTrace3 = DBApp.getFullTrace("b00x");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 155);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 141"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xn", cols0);
		String [][] records_xn = new String[393][cols0.length];
		for(int i=0;i<393;i++)
		{
			records_xn[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xn[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xn", records_xn[i]);
		}
	
		int pageCount0 = (int)Math.ceil(393.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("xn");
	
		DBApp.select("xn");
	
		DBApp.select("xn");
	
		DBApp.select("xn");
	
		DBApp.select("xn");
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c2"};
		DBApp.select("xn", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"e"};
		String[] ConditionColumnsValues08 = {"e1"};
		DBApp.select("xn", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"g"};
		String[] ConditionColumnsValues010 = {"g2"};
		DBApp.select("xn", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"f"};
		String[] ConditionColumnsValues011 = {"f1"};
		DBApp.select("xn", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("xn");
	
		String[] ConditionColumns014 = {"f"};
		String[] ConditionColumnsValues014 = {"f1"};
		DBApp.select("xn", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"b"};
		String[] ConditionColumnsValues015 = {"b0"};
		DBApp.select("xn", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"f"};
		String[] ConditionColumnsValues016 = {"f5"};
		DBApp.select("xn", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"d","e"};
		String[] ConditionColumnsValues017 = {"d3","e0"};
		DBApp.select("xn", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer019,recordSelectPointer019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((393%DBApp.dataPageSize))-1;
		DBApp.select("xn", pageSelectPointer020,recordSelectPointer020);
	
		String selectTrace0 = DBApp.getFullTrace("xn");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 416);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 393"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o5k", cols1);
		String [][] records_o5k = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			records_o5k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o5k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o5k", records_o5k[i]);
		}
	
		int pageCount1 = (int)Math.ceil(26.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((26%DBApp.dataPageSize))-1;
		DBApp.select("o5k", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((26%DBApp.dataPageSize))-1;
		DBApp.select("o5k", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("o5k");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((26%DBApp.dataPageSize))-1;
		DBApp.select("o5k", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("o5k");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((26%DBApp.dataPageSize))-1;
		DBApp.select("o5k", pageSelectPointer15,recordSelectPointer15);
	
		String selectTrace1 = DBApp.getFullTrace("o5k");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 34);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 26"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("yi49", cols0);
		String [][] records_yi49 = new String[406][cols0.length];
		for(int i=0;i<406;i++)
		{
			records_yi49[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yi49[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yi49", records_yi49[i]);
		}
	
		int pageCount0 = (int)Math.ceil(406.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((406%DBApp.dataPageSize))-1;
		DBApp.select("yi49", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("yi49");
	
		DBApp.select("yi49");
	
		DBApp.select("yi49");
	
		String[] ConditionColumns04 = {"b"};
		String[] ConditionColumnsValues04 = {"b1"};
		DBApp.select("yi49", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"e"};
		String[] ConditionColumnsValues05 = {"e2"};
		DBApp.select("yi49", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("yi49");
	
		DBApp.select("yi49");
	
		DBApp.select("yi49");
	
		String[] ConditionColumns09 = {"e"};
		String[] ConditionColumnsValues09 = {"e3"};
		DBApp.select("yi49", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("yi49");
	
		DBApp.select("yi49");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((406%DBApp.dataPageSize))-1;
		DBApp.select("yi49", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((406%DBApp.dataPageSize))-1;
		DBApp.select("yi49", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((406%DBApp.dataPageSize))-1;
		DBApp.select("yi49", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("yi49");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((406%DBApp.dataPageSize))-1;
		DBApp.select("yi49", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("yi49");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 425);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 406"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("hb3", cols1);
		String [][] records_hb3 = new String[428][cols1.length];
		for(int i=0;i<428;i++)
		{
			records_hb3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_hb3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("hb3", records_hb3[i]);
		}
	
		int pageCount1 = (int)Math.ceil(428.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns10 = {"b"};
		String[] ConditionColumnsValues10 = {"b0"};
		DBApp.select("hb3", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("hb3", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("hb3");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("hb3", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("hb3");
	
		String[] ConditionColumns15 = {"c"};
		String[] ConditionColumnsValues15 = {"c1"};
		DBApp.select("hb3", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"a"};
		String[] ConditionColumnsValues16 = {"a422"};
		DBApp.select("hb3", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((428%DBApp.dataPageSize))-1;
		DBApp.select("hb3", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("hb3");
	
		String selectTrace1 = DBApp.getFullTrace("hb3");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 439);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 428"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("p79", cols2);
		String [][] records_p79 = new String[412][cols2.length];
		for(int i=0;i<412;i++)
		{
			records_p79[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p79[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p79", records_p79[i]);
		}
	
		int pageCount2 = (int)Math.ceil(412.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("p79", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("p79", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("p79");
	
		String[] ConditionColumns23 = {"a"};
		String[] ConditionColumnsValues23 = {"a8"};
		DBApp.select("p79", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("p79", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("p79");
	
		String[] ConditionColumns26 = {"a"};
		String[] ConditionColumnsValues26 = {"a401"};
		DBApp.select("p79", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("p79", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((412%DBApp.dataPageSize))-1;
		DBApp.select("p79", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"a"};
		String[] ConditionColumnsValues29 = {"a63"};
		DBApp.select("p79", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("p79");
	
		DBApp.select("p79");
	
		String selectTrace2 = DBApp.getFullTrace("p79");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 426);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 412"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("ix1", cols0);
		String [][] records_ix1 = new String[316][cols0.length];
		for(int i=0;i<316;i++)
		{
			records_ix1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ix1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ix1", records_ix1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(316.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("ix1");
	
		DBApp.select("ix1");
	
		String[] ConditionColumns02 = {"b"};
		String[] ConditionColumnsValues02 = {"b1"};
		DBApp.select("ix1", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a266"};
		DBApp.select("ix1", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("ix1");
	
		DBApp.select("ix1");
	
		String[] ConditionColumns06 = {"b"};
		String[] ConditionColumnsValues06 = {"b0"};
		DBApp.select("ix1", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("ix1");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("ix1", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"b"};
		String[] ConditionColumnsValues09 = {"b1"};
		DBApp.select("ix1", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"a"};
		String[] ConditionColumnsValues010 = {"a15"};
		DBApp.select("ix1", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("ix1", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"b"};
		String[] ConditionColumnsValues012 = {"b0"};
		DBApp.select("ix1", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("ix1", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("ix1", pageSelectPointer014,recordSelectPointer014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("ix1", pageSelectPointer015,recordSelectPointer015);
	
		String selectTrace0 = DBApp.getFullTrace("ix1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 334);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 316"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("k6", cols0);
		String [][] records_k6 = new String[181][cols0.length];
		for(int i=0;i<181;i++)
		{
			records_k6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k6", records_k6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(181.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("k6");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"b"};
		String[] ConditionColumnsValues05 = {"b0"};
		DBApp.select("k6", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("k6");
	
		DBApp.select("k6");
	
		String[] ConditionColumns08 = {"f","c","e"};
		String[] ConditionColumnsValues08 = {"f2","c2","e1"};
		DBApp.select("k6", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"d"};
		String[] ConditionColumnsValues09 = {"d3"};
		DBApp.select("k6", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("k6");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("k6");
	
		DBApp.select("k6");
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer014,recordSelectPointer014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"e"};
		String[] ConditionColumnsValues017 = {"e4"};
		DBApp.select("k6", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"f"};
		String[] ConditionColumnsValues018 = {"f1"};
		DBApp.select("k6", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("k6");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((181%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer020,recordSelectPointer020);
	
		DBApp.select("k6");
	
		String selectTrace0 = DBApp.getFullTrace("k6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 205);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 181"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("oq0y8", cols1);
		String [][] records_oq0y8 = new String[285][cols1.length];
		for(int i=0;i<285;i++)
		{
			records_oq0y8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_oq0y8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("oq0y8", records_oq0y8[i]);
		}
	
		int pageCount1 = (int)Math.ceil(285.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("oq0y8");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("oq0y8", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("oq0y8");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("oq0y8", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("oq0y8", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("oq0y8", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("oq0y8", pageSelectPointer16,recordSelectPointer16);
	
		String selectTrace1 = DBApp.getFullTrace("oq0y8");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 294);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 285"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("ma", cols2);
		String [][] records_ma = new String[370][cols2.length];
		for(int i=0;i<370;i++)
		{
			records_ma[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ma[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ma", records_ma[i]);
		}
	
		int pageCount2 = (int)Math.ceil(370.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		DBApp.select("ma");
	
		DBApp.select("ma");
	
		String selectTrace2 = DBApp.getFullTrace("ma");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 374);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 370"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("q8", cols3);
		String [][] records_q8 = new String[285][cols3.length];
		for(int i=0;i<285;i++)
		{
			records_q8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q8", records_q8[i]);
		}
	
		int pageCount3 = (int)Math.ceil(285.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		String[] ConditionColumns30 = {"c"};
		String[] ConditionColumnsValues30 = {"c2"};
		DBApp.select("q8", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("q8");
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer32,recordSelectPointer32);
	
		String[] ConditionColumns33 = {"b"};
		String[] ConditionColumnsValues33 = {"b0"};
		DBApp.select("q8", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"b"};
		String[] ConditionColumnsValues35 = {"b0"};
		DBApp.select("q8", ConditionColumns35, ConditionColumnsValues35);
	
		DBApp.select("q8");
	
		DBApp.select("q8");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer38,recordSelectPointer38);
	
		String[] ConditionColumns39 = {"c"};
		String[] ConditionColumnsValues39 = {"c2"};
		DBApp.select("q8", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"b"};
		String[] ConditionColumnsValues310 = {"b1"};
		DBApp.select("q8", ConditionColumns310, ConditionColumnsValues310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer311,recordSelectPointer311);
	
		String[] ConditionColumns312 = {"d"};
		String[] ConditionColumnsValues312 = {"d3"};
		DBApp.select("q8", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer314,recordSelectPointer314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer315,recordSelectPointer315);
	
		String[] ConditionColumns316 = {"d"};
		String[] ConditionColumnsValues316 = {"d1"};
		DBApp.select("q8", ConditionColumns316, ConditionColumnsValues316);
	
		String[] ConditionColumns317 = {"a","b"};
		String[] ConditionColumnsValues317 = {"a261","b1"};
		DBApp.select("q8", ConditionColumns317, ConditionColumnsValues317);
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((285%DBApp.dataPageSize))-1;
		DBApp.select("q8", pageSelectPointer318,recordSelectPointer318);
	
		String[] ConditionColumns319 = {"a"};
		String[] ConditionColumnsValues319 = {"a142"};
		DBApp.select("q8", ConditionColumns319, ConditionColumnsValues319);
	
		String selectTrace3 = DBApp.getFullTrace("q8");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 307);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 285"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("hn664", cols0);
		String [][] records_hn664 = new String[458][cols0.length];
		for(int i=0;i<458;i++)
		{
			records_hn664[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hn664[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hn664", records_hn664[i]);
		}
	
		int pageCount0 = (int)Math.ceil(458.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("hn664");
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a54"};
		DBApp.select("hn664", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("hn664");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("hn664");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"b"};
		String[] ConditionColumnsValues06 = {"b0"};
		DBApp.select("hn664", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a208"};
		DBApp.select("hn664", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b1"};
		DBApp.select("hn664", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("hn664");
	
		String[] ConditionColumns012 = {"b"};
		String[] ConditionColumnsValues012 = {"b0"};
		DBApp.select("hn664", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("hn664");
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer014,recordSelectPointer014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((458%DBApp.dataPageSize))-1;
		DBApp.select("hn664", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("hn664");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 477);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 458"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("fu3h6", cols1);
		String [][] records_fu3h6 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			records_fu3h6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_fu3h6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("fu3h6", records_fu3h6[i]);
		}
	
		int pageCount1 = (int)Math.ceil(42.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("fu3h6", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"d"};
		String[] ConditionColumnsValues11 = {"d2"};
		DBApp.select("fu3h6", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("fu3h6", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("fu3h6");
	
		String[] ConditionColumns14 = {"i"};
		String[] ConditionColumnsValues14 = {"i6"};
		DBApp.select("fu3h6", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("fu3h6");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("fu3h6", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("fu3h6", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("fu3h6", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("fu3h6", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"g"};
		String[] ConditionColumnsValues110 = {"g2"};
		DBApp.select("fu3h6", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"g"};
		String[] ConditionColumnsValues111 = {"g4"};
		DBApp.select("fu3h6", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"c"};
		String[] ConditionColumnsValues112 = {"c0"};
		DBApp.select("fu3h6", ConditionColumns112, ConditionColumnsValues112);
	
		String[] ConditionColumns113 = {"f"};
		String[] ConditionColumnsValues113 = {"f2"};
		DBApp.select("fu3h6", ConditionColumns113, ConditionColumnsValues113);
	
		String selectTrace1 = DBApp.getFullTrace("fu3h6");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 58);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 42"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("l5k", cols2);
		String [][] records_l5k = new String[278][cols2.length];
		for(int i=0;i<278;i++)
		{
			records_l5k[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l5k[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l5k", records_l5k[i]);
		}
	
		int pageCount2 = (int)Math.ceil(278.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((278%DBApp.dataPageSize))-1;
		DBApp.select("l5k", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("l5k");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((278%DBApp.dataPageSize))-1;
		DBApp.select("l5k", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("l5k");
	
		String[] ConditionColumns24 = {"f"};
		String[] ConditionColumnsValues24 = {"f4"};
		DBApp.select("l5k", ConditionColumns24, ConditionColumnsValues24);
	
		String selectTrace2 = DBApp.getFullTrace("l5k");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 285);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 278"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("v2", cols0);
		String [][] records_v2 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			records_v2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v2", records_v2[i]);
		}
	
		int pageCount0 = (int)Math.ceil(84.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("v2");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"g"};
		String[] ConditionColumnsValues05 = {"g0"};
		DBApp.select("v2", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"e","f","c","d"};
		String[] ConditionColumnsValues07 = {"e1","f2","c2","d0"};
		DBApp.select("v2", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("v2");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("v2");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((84%DBApp.dataPageSize))-1;
		DBApp.select("v2", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("v2");
	
		DBApp.select("v2");
	
		String selectTrace0 = DBApp.getFullTrace("v2");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 100);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 84"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("omtv", cols1);
		String [][] records_omtv = new String[140][cols1.length];
		for(int i=0;i<140;i++)
		{
			records_omtv[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_omtv[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("omtv", records_omtv[i]);
		}
	
		int pageCount1 = (int)Math.ceil(140.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		DBApp.select("omtv");
	
		String[] ConditionColumns11 = {"b","a","g"};
		String[] ConditionColumnsValues11 = {"b1","a17","g3"};
		DBApp.select("omtv", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b1"};
		DBApp.select("omtv", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"f"};
		String[] ConditionColumnsValues18 = {"f1"};
		DBApp.select("omtv", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"c"};
		String[] ConditionColumnsValues19 = {"c2"};
		DBApp.select("omtv", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"h","f"};
		String[] ConditionColumnsValues110 = {"h5","f5"};
		DBApp.select("omtv", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"g"};
		String[] ConditionColumnsValues111 = {"g4"};
		DBApp.select("omtv", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("omtv");
	
		DBApp.select("omtv");
	
		String[] ConditionColumns115 = {"b"};
		String[] ConditionColumnsValues115 = {"b0"};
		DBApp.select("omtv", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"b"};
		String[] ConditionColumnsValues116 = {"b0"};
		DBApp.select("omtv", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("omtv");
	
		String[] ConditionColumns118 = {"d"};
		String[] ConditionColumnsValues118 = {"d1"};
		DBApp.select("omtv", ConditionColumns118, ConditionColumnsValues118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"c"};
		String[] ConditionColumnsValues120 = {"c1"};
		DBApp.select("omtv", ConditionColumns120, ConditionColumnsValues120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer121,recordSelectPointer121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer122,recordSelectPointer122);
	
		DBApp.select("omtv");
	
		DBApp.select("omtv");
	
		String[] ConditionColumns125 = {"f","h"};
		String[] ConditionColumnsValues125 = {"f4","h4"};
		DBApp.select("omtv", ConditionColumns125, ConditionColumnsValues125);
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer126,recordSelectPointer126);
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((140%DBApp.dataPageSize))-1;
		DBApp.select("omtv", pageSelectPointer127,recordSelectPointer127);
	
		String selectTrace1 = DBApp.getFullTrace("omtv");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 170);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 140"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("p90p", cols2);
		String [][] records_p90p = new String[427][cols2.length];
		for(int i=0;i<427;i++)
		{
			records_p90p[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_p90p[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("p90p", records_p90p[i]);
		}
	
		int pageCount2 = (int)Math.ceil(427.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"a"};
		String[] ConditionColumnsValues21 = {"a118"};
		DBApp.select("p90p", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("p90p");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer29,recordSelectPointer29);
	
		String[] ConditionColumns210 = {"a"};
		String[] ConditionColumnsValues210 = {"a133"};
		DBApp.select("p90p", ConditionColumns210, ConditionColumnsValues210);
	
		DBApp.select("p90p");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("p90p", pageSelectPointer214,recordSelectPointer214);
	
		DBApp.select("p90p");
	
		String[] ConditionColumns216 = {"a"};
		String[] ConditionColumnsValues216 = {"a68"};
		DBApp.select("p90p", ConditionColumns216, ConditionColumnsValues216);
	
		String selectTrace2 = DBApp.getFullTrace("p90p");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 446);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 427"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("e7", cols0);
		String [][] records_e7 = new String[415][cols0.length];
		for(int i=0;i<415;i++)
		{
			records_e7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e7", records_e7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(415.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"g"};
		String[] ConditionColumnsValues04 = {"g1"};
		DBApp.select("e7", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("e7");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"j"};
		String[] ConditionColumnsValues09 = {"j9"};
		DBApp.select("e7", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"c"};
		String[] ConditionColumnsValues011 = {"c1"};
		DBApp.select("e7", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((415%DBApp.dataPageSize))-1;
		DBApp.select("e7", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"j"};
		String[] ConditionColumnsValues013 = {"j2"};
		DBApp.select("e7", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"e","d"};
		String[] ConditionColumnsValues014 = {"e2","d3"};
		DBApp.select("e7", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"a"};
		String[] ConditionColumnsValues015 = {"a140"};
		DBApp.select("e7", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"f","i","b","e"};
		String[] ConditionColumnsValues016 = {"f0","i3","b0","e0"};
		DBApp.select("e7", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"i"};
		String[] ConditionColumnsValues017 = {"i4"};
		DBApp.select("e7", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("e7");
	
		DBApp.select("e7");
	
		String[] ConditionColumns020 = {"i"};
		String[] ConditionColumnsValues020 = {"i5"};
		DBApp.select("e7", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("e7");
	
		String selectTrace0 = DBApp.getFullTrace("e7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 439);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 415"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v86", cols0);
		String [][] records_v86 = new String[331][cols0.length];
		for(int i=0;i<331;i++)
		{
			records_v86[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v86[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v86", records_v86[i]);
		}
	
		int pageCount0 = (int)Math.ceil(331.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		String[] ConditionColumns00 = {"a","e","c"};
		String[] ConditionColumnsValues00 = {"a184","e4","c1"};
		DBApp.select("v86", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e0"};
		DBApp.select("v86", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("v86");
	
		String[] ConditionColumns03 = {"f"};
		String[] ConditionColumnsValues03 = {"f5"};
		DBApp.select("v86", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("v86", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("v86");
	
		String[] ConditionColumns06 = {"a","g","f"};
		String[] ConditionColumnsValues06 = {"a9","g2","f3"};
		DBApp.select("v86", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"e"};
		String[] ConditionColumnsValues07 = {"e2"};
		DBApp.select("v86", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("v86");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((331%DBApp.dataPageSize))-1;
		DBApp.select("v86", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"d"};
		String[] ConditionColumnsValues010 = {"d0"};
		DBApp.select("v86", ConditionColumns010, ConditionColumnsValues010);
	
		String selectTrace0 = DBApp.getFullTrace("v86");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 344);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 331"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("b1u7", cols1);
		String [][] records_b1u7 = new String[482][cols1.length];
		for(int i=0;i<482;i++)
		{
			records_b1u7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1u7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1u7", records_b1u7[i]);
		}
	
		int pageCount1 = (int)Math.ceil(482.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("b1u7");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"b"};
		String[] ConditionColumnsValues15 = {"b1"};
		DBApp.select("b1u7", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("b1u7");
	
		DBApp.select("b1u7");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b0"};
		DBApp.select("b1u7", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b0"};
		DBApp.select("b1u7", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a397"};
		DBApp.select("b1u7", ConditionColumns112, ConditionColumnsValues112);
	
		String[] ConditionColumns113 = {"b"};
		String[] ConditionColumnsValues113 = {"b1"};
		DBApp.select("b1u7", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((482%DBApp.dataPageSize))-1;
		DBApp.select("b1u7", pageSelectPointer114,recordSelectPointer114);
	
		String selectTrace1 = DBApp.getFullTrace("b1u7");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 499);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 482"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("ugv", cols2);
		String [][] records_ugv = new String[93][cols2.length];
		for(int i=0;i<93;i++)
		{
			records_ugv[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ugv[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ugv", records_ugv[i]);
		}
	
		int pageCount2 = (int)Math.ceil(93.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((93%DBApp.dataPageSize))-1;
		DBApp.select("ugv", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"b"};
		String[] ConditionColumnsValues21 = {"b0"};
		DBApp.select("ugv", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("ugv");
	
		DBApp.select("ugv");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((93%DBApp.dataPageSize))-1;
		DBApp.select("ugv", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((93%DBApp.dataPageSize))-1;
		DBApp.select("ugv", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((93%DBApp.dataPageSize))-1;
		DBApp.select("ugv", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("ugv");
	
		String selectTrace2 = DBApp.getFullTrace("ugv");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 103);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 93"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("kxt", cols3);
		String [][] records_kxt = new String[476][cols3.length];
		for(int i=0;i<476;i++)
		{
			records_kxt[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kxt[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kxt", records_kxt[i]);
		}
	
		int pageCount3 = (int)Math.ceil(476.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("kxt", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"f","h","b"};
		String[] ConditionColumnsValues31 = {"f3","h3","b1"};
		DBApp.select("kxt", ConditionColumns31, ConditionColumnsValues31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((476%DBApp.dataPageSize))-1;
		DBApp.select("kxt", pageSelectPointer32,recordSelectPointer32);
	
		String[] ConditionColumns33 = {"e"};
		String[] ConditionColumnsValues33 = {"e4"};
		DBApp.select("kxt", ConditionColumns33, ConditionColumnsValues33);
	
		String selectTrace3 = DBApp.getFullTrace("kxt");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 482);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 476"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("ohs", cols0);
		String [][] records_ohs = new String[297][cols0.length];
		for(int i=0;i<297;i++)
		{
			records_ohs[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ohs[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ohs", records_ohs[i]);
		}
	
		int pageCount0 = (int)Math.ceil(297.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		String[] ConditionColumns00 = {"b"};
		String[] ConditionColumnsValues00 = {"b1"};
		DBApp.select("ohs", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a168"};
		DBApp.select("ohs", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((297%DBApp.dataPageSize))-1;
		DBApp.select("ohs", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("ohs");
	
		DBApp.select("ohs");
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c1"};
		DBApp.select("ohs", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((297%DBApp.dataPageSize))-1;
		DBApp.select("ohs", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c1"};
		DBApp.select("ohs", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("ohs");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((297%DBApp.dataPageSize))-1;
		DBApp.select("ohs", pageSelectPointer09,recordSelectPointer09);
	
		String selectTrace0 = DBApp.getFullTrace("ohs");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 309);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 297"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("v813t", cols1);
		String [][] records_v813t = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			records_v813t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v813t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v813t", records_v813t[i]);
		}
	
		int pageCount1 = (int)Math.ceil(57.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		DBApp.select("v813t");
	
		DBApp.select("v813t");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("v813t");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a34"};
		DBApp.select("v813t", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("v813t");
	
		DBApp.select("v813t");
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b0"};
		DBApp.select("v813t", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("v813t");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer114,recordSelectPointer114);
	
		String[] ConditionColumns115 = {"b"};
		String[] ConditionColumnsValues115 = {"b0"};
		DBApp.select("v813t", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("v813t");
	
		DBApp.select("v813t");
	
		String[] ConditionColumns119 = {"b"};
		String[] ConditionColumnsValues119 = {"b1"};
		DBApp.select("v813t", ConditionColumns119, ConditionColumnsValues119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((57%DBApp.dataPageSize))-1;
		DBApp.select("v813t", pageSelectPointer120,recordSelectPointer120);
	
		String[] ConditionColumns121 = {"c"};
		String[] ConditionColumnsValues121 = {"c1"};
		DBApp.select("v813t", ConditionColumns121, ConditionColumnsValues121);
	
		DBApp.select("v813t");
	
		String[] ConditionColumns123 = {"b"};
		String[] ConditionColumnsValues123 = {"b1"};
		DBApp.select("v813t", ConditionColumns123, ConditionColumnsValues123);
	
		String selectTrace1 = DBApp.getFullTrace("v813t");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 83);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 57"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("m42d4", cols0);
		String [][] records_m42d4 = new String[245][cols0.length];
		for(int i=0;i<245;i++)
		{
			records_m42d4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m42d4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m42d4", records_m42d4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(245.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		DBApp.select("m42d4");
	
		DBApp.select("m42d4");
	
		DBApp.select("m42d4");
	
		String[] ConditionColumns03 = {"j","h","a","g","f"};
		String[] ConditionColumnsValues03 = {"j7","h1","a137","g4","f5"};
		DBApp.select("m42d4", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"b"};
		String[] ConditionColumnsValues04 = {"b0"};
		DBApp.select("m42d4", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"i","b","f"};
		String[] ConditionColumnsValues05 = {"i6","b1","f3"};
		DBApp.select("m42d4", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("m42d4");
	
		DBApp.select("m42d4");
	
		String selectTrace0 = DBApp.getFullTrace("m42d4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 255);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 245"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("kg79i", cols1);
		String [][] records_kg79i = new String[196][cols1.length];
		for(int i=0;i<196;i++)
		{
			records_kg79i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kg79i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kg79i", records_kg79i[i]);
		}
	
		int pageCount1 = (int)Math.ceil(196.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns10 = {"c"};
		String[] ConditionColumnsValues10 = {"c2"};
		DBApp.select("kg79i", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"d"};
		String[] ConditionColumnsValues11 = {"d0"};
		DBApp.select("kg79i", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("kg79i", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"k"};
		String[] ConditionColumnsValues13 = {"k5"};
		DBApp.select("kg79i", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("kg79i", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("kg79i");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("kg79i", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"e"};
		String[] ConditionColumnsValues17 = {"e0"};
		DBApp.select("kg79i", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("kg79i");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("kg79i", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"d"};
		String[] ConditionColumnsValues110 = {"d3"};
		DBApp.select("kg79i", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("kg79i");
	
		DBApp.select("kg79i");
	
		DBApp.select("kg79i");
	
		String[] ConditionColumns114 = {"d"};
		String[] ConditionColumnsValues114 = {"d1"};
		DBApp.select("kg79i", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((196%DBApp.dataPageSize))-1;
		DBApp.select("kg79i", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("kg79i");
	
		String[] ConditionColumns117 = {"e"};
		String[] ConditionColumnsValues117 = {"e3"};
		DBApp.select("kg79i", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"i"};
		String[] ConditionColumnsValues118 = {"i6"};
		DBApp.select("kg79i", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"c"};
		String[] ConditionColumnsValues119 = {"c0"};
		DBApp.select("kg79i", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("kg79i");
	
		DBApp.select("kg79i");
	
		String[] ConditionColumns122 = {"h","b","l","a"};
		String[] ConditionColumnsValues122 = {"h7","b1","l11","a47"};
		DBApp.select("kg79i", ConditionColumns122, ConditionColumnsValues122);
	
		String selectTrace1 = DBApp.getFullTrace("kg79i");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 221);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 196"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ji", cols2);
		String [][] records_ji = new String[117][cols2.length];
		for(int i=0;i<117;i++)
		{
			records_ji[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ji[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ji", records_ji[i]);
		}
	
		int pageCount2 = (int)Math.ceil(117.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((117%DBApp.dataPageSize))-1;
		DBApp.select("ji", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"b"};
		String[] ConditionColumnsValues21 = {"b1"};
		DBApp.select("ji", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"k","a","l"};
		String[] ConditionColumnsValues22 = {"k10","a98","l2"};
		DBApp.select("ji", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("ji");
	
		String[] ConditionColumns24 = {"d","f","a","g"};
		String[] ConditionColumnsValues24 = {"d3","f1","a55","g6"};
		DBApp.select("ji", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"d","k","e","l"};
		String[] ConditionColumnsValues25 = {"d0","k10","e1","l4"};
		DBApp.select("ji", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"f"};
		String[] ConditionColumnsValues26 = {"f0"};
		DBApp.select("ji", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("ji");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((117%DBApp.dataPageSize))-1;
		DBApp.select("ji", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("ji");
	
		DBApp.select("ji");
	
		DBApp.select("ji");
	
		String selectTrace2 = DBApp.getFullTrace("ji");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 131);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 117"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("b0q", cols3);
		String [][] records_b0q = new String[259][cols3.length];
		for(int i=0;i<259;i++)
		{
			records_b0q[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b0q[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b0q", records_b0q[i]);
		}
	
		int pageCount3 = (int)Math.ceil(259.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns30 = {"j"};
		String[] ConditionColumnsValues30 = {"j6"};
		DBApp.select("b0q", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("b0q");
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((259%DBApp.dataPageSize))-1;
		DBApp.select("b0q", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("b0q");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((259%DBApp.dataPageSize))-1;
		DBApp.select("b0q", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"n"};
		String[] ConditionColumnsValues35 = {"n10"};
		DBApp.select("b0q", ConditionColumns35, ConditionColumnsValues35);
	
		String[] ConditionColumns36 = {"a"};
		String[] ConditionColumnsValues36 = {"a157"};
		DBApp.select("b0q", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("b0q");
	
		String[] ConditionColumns38 = {"g","k","n","e","m"};
		String[] ConditionColumnsValues38 = {"g4","k5","n4","e4","m6"};
		DBApp.select("b0q", ConditionColumns38, ConditionColumnsValues38);
	
		String selectTrace3 = DBApp.getFullTrace("b0q");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 270);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 259"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("a95", cols0);
		String [][] records_a95 = new String[318][cols0.length];
		for(int i=0;i<318;i++)
		{
			records_a95[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a95[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a95", records_a95[i]);
		}
	
		int pageCount0 = (int)Math.ceil(318.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((318%DBApp.dataPageSize))-1;
		DBApp.select("a95", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a252"};
		DBApp.select("a95", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("a95");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((318%DBApp.dataPageSize))-1;
		DBApp.select("a95", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a55"};
		DBApp.select("a95", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("a95");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((318%DBApp.dataPageSize))-1;
		DBApp.select("a95", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((318%DBApp.dataPageSize))-1;
		DBApp.select("a95", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"a"};
		String[] ConditionColumnsValues08 = {"a184"};
		DBApp.select("a95", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a289"};
		DBApp.select("a95", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"a"};
		String[] ConditionColumnsValues010 = {"a207"};
		DBApp.select("a95", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"a"};
		String[] ConditionColumnsValues011 = {"a207"};
		DBApp.select("a95", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("a95");
	
		DBApp.select("a95");
	
		DBApp.select("a95");
	
		DBApp.select("a95");
	
		DBApp.select("a95");
	
		DBApp.select("a95");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((318%DBApp.dataPageSize))-1;
		DBApp.select("a95", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("a95");
	
		String selectTrace0 = DBApp.getFullTrace("a95");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 340);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 318"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("h6v", cols1);
		String [][] records_h6v = new String[450][cols1.length];
		for(int i=0;i<450;i++)
		{
			records_h6v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h6v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h6v", records_h6v[i]);
		}
	
		int pageCount1 = (int)Math.ceil(450.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		DBApp.select("h6v");
	
		String[] ConditionColumns11 = {"h"};
		String[] ConditionColumnsValues11 = {"h2"};
		DBApp.select("h6v", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"h","k","e","d"};
		String[] ConditionColumnsValues12 = {"h3","k8","e3","d3"};
		DBApp.select("h6v", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("h6v");
	
		String[] ConditionColumns14 = {"j","b","f","c","k"};
		String[] ConditionColumnsValues14 = {"j3","b1","f5","c2","k7"};
		DBApp.select("h6v", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("h6v");
	
		DBApp.select("h6v");
	
		String[] ConditionColumns17 = {"d"};
		String[] ConditionColumnsValues17 = {"d1"};
		DBApp.select("h6v", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((450%DBApp.dataPageSize))-1;
		DBApp.select("h6v", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("h6v");
	
		DBApp.select("h6v");
	
		String[] ConditionColumns111 = {"c"};
		String[] ConditionColumnsValues111 = {"c2"};
		DBApp.select("h6v", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("h6v");
	
		String[] ConditionColumns113 = {"j"};
		String[] ConditionColumnsValues113 = {"j4"};
		DBApp.select("h6v", ConditionColumns113, ConditionColumnsValues113);
	
		String selectTrace1 = DBApp.getFullTrace("h6v");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 466);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 450"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("eu0l", cols2);
		String [][] records_eu0l = new String[431][cols2.length];
		for(int i=0;i<431;i++)
		{
			records_eu0l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_eu0l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("eu0l", records_eu0l[i]);
		}
	
		int pageCount2 = (int)Math.ceil(431.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		String[] ConditionColumns20 = {"k","e","g","f"};
		String[] ConditionColumnsValues20 = {"k1","e4","g0","f4"};
		DBApp.select("eu0l", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("eu0l");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("eu0l", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("eu0l");
	
		String[] ConditionColumns24 = {"l","f","k"};
		String[] ConditionColumnsValues24 = {"l5","f5","k1"};
		DBApp.select("eu0l", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("eu0l", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"c","i"};
		String[] ConditionColumnsValues26 = {"c2","i8"};
		DBApp.select("eu0l", ConditionColumns26, ConditionColumnsValues26);
	
		String[] ConditionColumns27 = {"a"};
		String[] ConditionColumnsValues27 = {"a389"};
		DBApp.select("eu0l", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"h"};
		String[] ConditionColumnsValues28 = {"h2"};
		DBApp.select("eu0l", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("eu0l");
	
		String selectTrace2 = DBApp.getFullTrace("eu0l");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 443);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 431"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m4c", cols0);
		String [][] records_m4c = new String[397][cols0.length];
		for(int i=0;i<397;i++)
		{
			records_m4c[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m4c[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m4c", records_m4c[i]);
		}
	
		int pageCount0 = (int)Math.ceil(397.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("m4c", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("m4c", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("m4c", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("m4c");
	
		DBApp.select("m4c");
	
		String[] ConditionColumns05 = {"k"};
		String[] ConditionColumnsValues05 = {"k9"};
		DBApp.select("m4c", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("m4c");
	
		DBApp.select("m4c");
	
		DBApp.select("m4c");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("m4c", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"i"};
		String[] ConditionColumnsValues010 = {"i4"};
		DBApp.select("m4c", ConditionColumns010, ConditionColumnsValues010);
	
		String selectTrace0 = DBApp.getFullTrace("m4c");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 410);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 397"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("we5", cols1);
		String [][] records_we5 = new String[256][cols1.length];
		for(int i=0;i<256;i++)
		{
			records_we5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_we5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("we5", records_we5[i]);
		}
	
		int pageCount1 = (int)Math.ceil(256.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		String[] ConditionColumns10 = {"a"};
		String[] ConditionColumnsValues10 = {"a205"};
		DBApp.select("we5", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("we5", pageSelectPointer11,recordSelectPointer11);
	
		String selectTrace1 = DBApp.getFullTrace("we5");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 260);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 256"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kdg5", cols0);
		String [][] records_kdg5 = new String[112][cols0.length];
		for(int i=0;i<112;i++)
		{
			records_kdg5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kdg5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kdg5", records_kdg5[i]);
		}
	
		int pageCount0 = (int)Math.ceil(112.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("kdg5", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("kdg5", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"f"};
		String[] ConditionColumnsValues02 = {"f2"};
		DBApp.select("kdg5", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("kdg5", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((112%DBApp.dataPageSize))-1;
		DBApp.select("kdg5", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("kdg5");
	
		DBApp.select("kdg5");
	
		String[] ConditionColumns07 = {"e"};
		String[] ConditionColumnsValues07 = {"e0"};
		DBApp.select("kdg5", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("kdg5");
	
		DBApp.select("kdg5");
	
		String selectTrace0 = DBApp.getFullTrace("kdg5");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 124);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 112"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qbb52", cols0);
		String [][] records_qbb52 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			records_qbb52[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qbb52[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qbb52", records_qbb52[i]);
		}
	
		int pageCount0 = (int)Math.ceil(16.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns01 = {"g"};
		String[] ConditionColumnsValues01 = {"g1"};
		DBApp.select("qbb52", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("qbb52");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((16%DBApp.dataPageSize))-1;
		DBApp.select("qbb52", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns05 = {"b","e","h"};
		String[] ConditionColumnsValues05 = {"b1","e1","h3"};
		DBApp.select("qbb52", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((16%DBApp.dataPageSize))-1;
		DBApp.select("qbb52", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("qbb52");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((16%DBApp.dataPageSize))-1;
		DBApp.select("qbb52", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns010 = {"h","f","b"};
		String[] ConditionColumnsValues010 = {"h1","f1","b1"};
		DBApp.select("qbb52", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns012 = {"a"};
		String[] ConditionColumnsValues012 = {"a7"};
		DBApp.select("qbb52", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("qbb52");
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns015 = {"b","g","h"};
		String[] ConditionColumnsValues015 = {"b1","g3","h3"};
		DBApp.select("qbb52", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"g"};
		String[] ConditionColumnsValues016 = {"g1"};
		DBApp.select("qbb52", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("qbb52");
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns019 = {"g"};
		String[] ConditionColumnsValues019 = {"g1"};
		DBApp.select("qbb52", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("qbb52");
	
		DBApp.select("qbb52");
	
		String[] ConditionColumns022 = {"i"};
		String[] ConditionColumnsValues022 = {"i4"};
		DBApp.select("qbb52", ConditionColumns022, ConditionColumnsValues022);
	
		String[] ConditionColumns023 = {"i"};
		String[] ConditionColumnsValues023 = {"i4"};
		DBApp.select("qbb52", ConditionColumns023, ConditionColumnsValues023);
	
		String[] ConditionColumns024 = {"c"};
		String[] ConditionColumnsValues024 = {"c1"};
		DBApp.select("qbb52", ConditionColumns024, ConditionColumnsValues024);
	
		String[] ConditionColumns025 = {"a"};
		String[] ConditionColumnsValues025 = {"a13"};
		DBApp.select("qbb52", ConditionColumns025, ConditionColumnsValues025);
	
		DBApp.select("qbb52");
	
		DBApp.select("qbb52");
	
		String selectTrace0 = DBApp.getFullTrace("qbb52");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 46);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 16"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("oj8", cols0);
		String [][] records_oj8 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_oj8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oj8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oj8", records_oj8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(38.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		DBApp.select("oj8");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("oj8", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"k"};
		String[] ConditionColumnsValues02 = {"k1"};
		DBApp.select("oj8", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"j","b","g"};
		String[] ConditionColumnsValues03 = {"j4","b0","g0"};
		DBApp.select("oj8", ConditionColumns03, ConditionColumnsValues03);
	
		String selectTrace0 = DBApp.getFullTrace("oj8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 44);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 38"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g37", cols1);
		String [][] records_g37 = new String[115][cols1.length];
		for(int i=0;i<115;i++)
		{
			records_g37[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g37[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g37", records_g37[i]);
		}
	
		int pageCount1 = (int)Math.ceil(115.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("g37");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((115%DBApp.dataPageSize))-1;
		DBApp.select("g37", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("g37");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((115%DBApp.dataPageSize))-1;
		DBApp.select("g37", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((115%DBApp.dataPageSize))-1;
		DBApp.select("g37", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((115%DBApp.dataPageSize))-1;
		DBApp.select("g37", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("g37");
	
		DBApp.select("g37");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((115%DBApp.dataPageSize))-1;
		DBApp.select("g37", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((115%DBApp.dataPageSize))-1;
		DBApp.select("g37", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b1"};
		DBApp.select("g37", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"e"};
		String[] ConditionColumnsValues111 = {"e4"};
		DBApp.select("g37", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"f"};
		String[] ConditionColumnsValues112 = {"f2"};
		DBApp.select("g37", ConditionColumns112, ConditionColumnsValues112);
	
		String[] ConditionColumns113 = {"f"};
		String[] ConditionColumnsValues113 = {"f5"};
		DBApp.select("g37", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("g37");
	
		String[] ConditionColumns115 = {"d"};
		String[] ConditionColumnsValues115 = {"d2"};
		DBApp.select("g37", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("g37");
	
		String selectTrace1 = DBApp.getFullTrace("g37");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 134);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 115"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("l8l", cols2);
		String [][] records_l8l = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			records_l8l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_l8l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("l8l", records_l8l[i]);
		}
	
		int pageCount2 = (int)Math.ceil(48.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns20 = {"d","c"};
		String[] ConditionColumnsValues20 = {"d2","c1"};
		DBApp.select("l8l", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("l8l", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("l8l");
	
		String[] ConditionColumns23 = {"c"};
		String[] ConditionColumnsValues23 = {"c2"};
		DBApp.select("l8l", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("l8l", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("l8l", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("l8l");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("l8l", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("l8l");
	
		String[] ConditionColumns29 = {"e"};
		String[] ConditionColumnsValues29 = {"e2"};
		DBApp.select("l8l", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("l8l");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("l8l", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("l8l");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("l8l", pageSelectPointer213,recordSelectPointer213);
	
		String selectTrace2 = DBApp.getFullTrace("l8l");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 64);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 48"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("qs", cols0);
		String [][] records_qs = new String[124][cols0.length];
		for(int i=0;i<124;i++)
		{
			records_qs[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qs[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qs", records_qs[i]);
		}
	
		int pageCount0 = (int)Math.ceil(124.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a120"};
		DBApp.select("qs", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("qs");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("qs", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("qs", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("qs");
	
		DBApp.select("qs");
	
		DBApp.select("qs");
	
		DBApp.select("qs");
	
		String[] ConditionColumns08 = {"b"};
		String[] ConditionColumnsValues08 = {"b0"};
		DBApp.select("qs", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a17"};
		DBApp.select("qs", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("qs", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("qs");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("qs", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("qs", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("qs", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("qs");
	
		DBApp.select("qs");
	
		DBApp.select("qs");
	
		String[] ConditionColumns018 = {"a"};
		String[] ConditionColumnsValues018 = {"a67"};
		DBApp.select("qs", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"b"};
		String[] ConditionColumnsValues019 = {"b1"};
		DBApp.select("qs", ConditionColumns019, ConditionColumnsValues019);
	
		String selectTrace0 = DBApp.getFullTrace("qs");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 146);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 124"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l2", cols1);
		String [][] records_l2 = new String[397][cols1.length];
		for(int i=0;i<397;i++)
		{
			records_l2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l2", records_l2[i]);
		}
	
		int pageCount1 = (int)Math.ceil(397.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"j"};
		String[] ConditionColumnsValues12 = {"j2"};
		DBApp.select("l2", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"e"};
		String[] ConditionColumnsValues19 = {"e2"};
		DBApp.select("l2", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("l2");
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"k"};
		String[] ConditionColumnsValues113 = {"k5"};
		DBApp.select("l2", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("l2");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("l2", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"i"};
		String[] ConditionColumnsValues117 = {"i2"};
		DBApp.select("l2", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"f"};
		String[] ConditionColumnsValues118 = {"f1"};
		DBApp.select("l2", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"m","a","i","e","b","d"};
		String[] ConditionColumnsValues119 = {"m7","a228","i3","e3","b0","d0"};
		DBApp.select("l2", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("l2");
	
		String[] ConditionColumns121 = {"i"};
		String[] ConditionColumnsValues121 = {"i6"};
		DBApp.select("l2", ConditionColumns121, ConditionColumnsValues121);
	
		String selectTrace1 = DBApp.getFullTrace("l2");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 421);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 397"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gokn8", cols2);
		String [][] records_gokn8 = new String[272][cols2.length];
		for(int i=0;i<272;i++)
		{
			records_gokn8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_gokn8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("gokn8", records_gokn8[i]);
		}
	
		int pageCount2 = (int)Math.ceil(272.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"f"};
		String[] ConditionColumnsValues21 = {"f4"};
		DBApp.select("gokn8", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("gokn8");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("gokn8");
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer26,recordSelectPointer26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"g"};
		String[] ConditionColumnsValues28 = {"g2"};
		DBApp.select("gokn8", ConditionColumns28, ConditionColumnsValues28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("gokn8");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"c"};
		String[] ConditionColumnsValues215 = {"c1"};
		DBApp.select("gokn8", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((272%DBApp.dataPageSize))-1;
		DBApp.select("gokn8", pageSelectPointer216,recordSelectPointer216);
	
		DBApp.select("gokn8");
	
		DBApp.select("gokn8");
	
		DBApp.select("gokn8");
	
		String[] ConditionColumns220 = {"f","d"};
		String[] ConditionColumnsValues220 = {"f5","d3"};
		DBApp.select("gokn8", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"c"};
		String[] ConditionColumnsValues221 = {"c0"};
		DBApp.select("gokn8", ConditionColumns221, ConditionColumnsValues221);
	
		String selectTrace2 = DBApp.getFullTrace("gokn8");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 296);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 272"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("y353", cols3);
		String [][] records_y353 = new String[114][cols3.length];
		for(int i=0;i<114;i++)
		{
			records_y353[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y353[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y353", records_y353[i]);
		}
	
		int pageCount3 = (int)Math.ceil(114.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y353", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"a"};
		String[] ConditionColumnsValues31 = {"a105"};
		DBApp.select("y353", ConditionColumns31, ConditionColumnsValues31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y353", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		String[] ConditionColumns39 = {"a"};
		String[] ConditionColumnsValues39 = {"a49"};
		DBApp.select("y353", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"a"};
		String[] ConditionColumnsValues310 = {"a90"};
		DBApp.select("y353", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"a"};
		String[] ConditionColumnsValues311 = {"a35"};
		DBApp.select("y353", ConditionColumns311, ConditionColumnsValues311);
	
		DBApp.select("y353");
	
		String[] ConditionColumns313 = {"a"};
		String[] ConditionColumnsValues313 = {"a83"};
		DBApp.select("y353", ConditionColumns313, ConditionColumnsValues313);
	
		String[] ConditionColumns314 = {"a"};
		String[] ConditionColumnsValues314 = {"a62"};
		DBApp.select("y353", ConditionColumns314, ConditionColumnsValues314);
	
		String[] ConditionColumns315 = {"a"};
		String[] ConditionColumnsValues315 = {"a55"};
		DBApp.select("y353", ConditionColumns315, ConditionColumnsValues315);
	
		String[] ConditionColumns316 = {"a"};
		String[] ConditionColumnsValues316 = {"a37"};
		DBApp.select("y353", ConditionColumns316, ConditionColumnsValues316);
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		DBApp.select("y353");
	
		String[] ConditionColumns320 = {"a"};
		String[] ConditionColumnsValues320 = {"a17"};
		DBApp.select("y353", ConditionColumns320, ConditionColumnsValues320);
	
		String[] ConditionColumns321 = {"a"};
		String[] ConditionColumnsValues321 = {"a112"};
		DBApp.select("y353", ConditionColumns321, ConditionColumnsValues321);
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y353", pageSelectPointer322,recordSelectPointer322);
	
		DBApp.select("y353");
	
		int pageSelectPointer324 = genRandNum(pageCount3)-1;
		int recordSelectPointer324 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer324==pageCount3-1)
			recordSelectPointer324 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y353", pageSelectPointer324,recordSelectPointer324);
	
		DBApp.select("y353");
	
		String[] ConditionColumns326 = {"a"};
		String[] ConditionColumnsValues326 = {"a3"};
		DBApp.select("y353", ConditionColumns326, ConditionColumnsValues326);
	
		DBApp.select("y353");
	
		int pageSelectPointer328 = genRandNum(pageCount3)-1;
		int recordSelectPointer328 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer328==pageCount3-1)
			recordSelectPointer328 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y353", pageSelectPointer328,recordSelectPointer328);
	
		String selectTrace3 = DBApp.getFullTrace("y353");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 145);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 114"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("bzoch", cols4);
		String [][] records_bzoch = new String[96][cols4.length];
		for(int i=0;i<96;i++)
		{
			records_bzoch[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_bzoch[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("bzoch", records_bzoch[i]);
		}
	
		int pageCount4 = (int)Math.ceil(96.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("bzoch");
	
		DBApp.select("bzoch");
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("bzoch", pageSelectPointer42,recordSelectPointer42);
	
		DBApp.select("bzoch");
	
		DBApp.select("bzoch");
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("bzoch", pageSelectPointer45,recordSelectPointer45);
	
		String[] ConditionColumns46 = {"a"};
		String[] ConditionColumnsValues46 = {"a4"};
		DBApp.select("bzoch", ConditionColumns46, ConditionColumnsValues46);
	
		DBApp.select("bzoch");
	
		DBApp.select("bzoch");
	
		String[] ConditionColumns49 = {"a"};
		String[] ConditionColumnsValues49 = {"a44"};
		DBApp.select("bzoch", ConditionColumns49, ConditionColumnsValues49);
	
		int pageSelectPointer410 = genRandNum(pageCount4)-1;
		int recordSelectPointer410 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer410==pageCount4-1)
			recordSelectPointer410 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("bzoch", pageSelectPointer410,recordSelectPointer410);
	
		DBApp.select("bzoch");
	
		int pageSelectPointer412 = genRandNum(pageCount4)-1;
		int recordSelectPointer412 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer412==pageCount4-1)
			recordSelectPointer412 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("bzoch", pageSelectPointer412,recordSelectPointer412);
	
		DBApp.select("bzoch");
	
		String[] ConditionColumns414 = {"a"};
		String[] ConditionColumnsValues414 = {"a88"};
		DBApp.select("bzoch", ConditionColumns414, ConditionColumnsValues414);
	
		int pageSelectPointer415 = genRandNum(pageCount4)-1;
		int recordSelectPointer415 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer415==pageCount4-1)
			recordSelectPointer415 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("bzoch", pageSelectPointer415,recordSelectPointer415);
	
		String selectTrace4 = DBApp.getFullTrace("bzoch");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 114);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 96"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("a06", cols0);
		String [][] records_a06 = new String[343][cols0.length];
		for(int i=0;i<343;i++)
		{
			records_a06[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a06[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a06", records_a06[i]);
		}
	
		int pageCount0 = (int)Math.ceil(343.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		DBApp.select("a06");
	
		String[] ConditionColumns01 = {"m","j","b"};
		String[] ConditionColumnsValues01 = {"m3","j3","b1"};
		DBApp.select("a06", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("a06");
	
		String[] ConditionColumns03 = {"h","l","k","m","g","a"};
		String[] ConditionColumnsValues03 = {"h1","l1","k7","m8","g3","a73"};
		DBApp.select("a06", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("a06");
	
		String[] ConditionColumns05 = {"f"};
		String[] ConditionColumnsValues05 = {"f1"};
		DBApp.select("a06", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("a06", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c1"};
		DBApp.select("a06", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"g","j","k"};
		String[] ConditionColumnsValues08 = {"g2","j0","k9"};
		DBApp.select("a06", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"d","e","c","i","h"};
		String[] ConditionColumnsValues09 = {"d2","e0","c1","i7","h2"};
		DBApp.select("a06", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("a06", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("a06", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("a06", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("a06");
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("a06", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"k","f","g","e","o","l","h"};
		String[] ConditionColumnsValues015 = {"k2","f5","g6","e2","o2","l11","h7"};
		DBApp.select("a06", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("a06");
	
		String[] ConditionColumns017 = {"c","o","a","m","d","k","l"};
		String[] ConditionColumnsValues017 = {"c2","o11","a311","m12","d3","k3","l11"};
		DBApp.select("a06", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"h"};
		String[] ConditionColumnsValues018 = {"h3"};
		DBApp.select("a06", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("a06");
	
		String[] ConditionColumns020 = {"h"};
		String[] ConditionColumnsValues020 = {"h7"};
		DBApp.select("a06", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("a06");
	
		DBApp.select("a06");
	
		DBApp.select("a06");
	
		String[] ConditionColumns024 = {"h"};
		String[] ConditionColumnsValues024 = {"h3"};
		DBApp.select("a06", ConditionColumns024, ConditionColumnsValues024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((343%DBApp.dataPageSize))-1;
		DBApp.select("a06", pageSelectPointer025,recordSelectPointer025);
	
		String[] ConditionColumns026 = {"g"};
		String[] ConditionColumnsValues026 = {"g6"};
		DBApp.select("a06", ConditionColumns026, ConditionColumnsValues026);
	
		String selectTrace0 = DBApp.getFullTrace("a06");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 372);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 343"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("l0h7v", cols1);
		String [][] records_l0h7v = new String[489][cols1.length];
		for(int i=0;i<489;i++)
		{
			records_l0h7v[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l0h7v[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l0h7v", records_l0h7v[i]);
		}
	
		int pageCount1 = (int)Math.ceil(489.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((489%DBApp.dataPageSize))-1;
		DBApp.select("l0h7v", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"l"};
		String[] ConditionColumnsValues11 = {"l10"};
		DBApp.select("l0h7v", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"d"};
		String[] ConditionColumnsValues12 = {"d3"};
		DBApp.select("l0h7v", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((489%DBApp.dataPageSize))-1;
		DBApp.select("l0h7v", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"k","f","c"};
		String[] ConditionColumnsValues14 = {"k1","f5","c2"};
		DBApp.select("l0h7v", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"j","f","c","e"};
		String[] ConditionColumnsValues15 = {"j0","f2","c2","e0"};
		DBApp.select("l0h7v", ConditionColumns15, ConditionColumnsValues15);
	
		String selectTrace1 = DBApp.getFullTrace("l0h7v");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 497);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 489"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("watq4", cols2);
		String [][] records_watq4 = new String[33][cols2.length];
		for(int i=0;i<33;i++)
		{
			records_watq4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_watq4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("watq4", records_watq4[i]);
		}
	
		int pageCount2 = (int)Math.ceil(33.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns20 = {"c","g","h","k","f"};
		String[] ConditionColumnsValues20 = {"c1","g4","h4","k4","f4"};
		DBApp.select("watq4", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((33%DBApp.dataPageSize))-1;
		DBApp.select("watq4", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"b"};
		String[] ConditionColumnsValues22 = {"b0"};
		DBApp.select("watq4", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((33%DBApp.dataPageSize))-1;
		DBApp.select("watq4", pageSelectPointer23,recordSelectPointer23);
	
		String selectTrace2 = DBApp.getFullTrace("watq4");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 39);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 33"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("umo", cols3);
		String [][] records_umo = new String[51][cols3.length];
		for(int i=0;i<51;i++)
		{
			records_umo[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_umo[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("umo", records_umo[i]);
		}
	
		int pageCount3 = (int)Math.ceil(51.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		String[] ConditionColumns30 = {"g"};
		String[] ConditionColumnsValues30 = {"g2"};
		DBApp.select("umo", ConditionColumns30, ConditionColumnsValues30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((51%DBApp.dataPageSize))-1;
		DBApp.select("umo", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"g"};
		String[] ConditionColumnsValues32 = {"g2"};
		DBApp.select("umo", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("umo");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((51%DBApp.dataPageSize))-1;
		DBApp.select("umo", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("umo");
	
		String selectTrace3 = DBApp.getFullTrace("umo");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 59);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 51"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("enk", cols0);
		String [][] records_enk = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_enk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_enk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("enk", records_enk[i]);
		}
	
		int pageCount0 = (int)Math.ceil(10.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a1"};
		DBApp.select("enk", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((10%DBApp.dataPageSize))-1;
		DBApp.select("enk", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"l"};
		String[] ConditionColumnsValues02 = {"l2"};
		DBApp.select("enk", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((10%DBApp.dataPageSize))-1;
		DBApp.select("enk", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((10%DBApp.dataPageSize))-1;
		DBApp.select("enk", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("enk");
	
		DBApp.select("enk");
	
		String[] ConditionColumns07 = {"l","o","n","k","j"};
		String[] ConditionColumnsValues07 = {"l5","o5","n5","k5","j5"};
		DBApp.select("enk", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("enk");
	
		String[] ConditionColumns09 = {"i","l","a"};
		String[] ConditionColumnsValues09 = {"i6","l6","a6"};
		DBApp.select("enk", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("enk");
	
		String[] ConditionColumns011 = {"k"};
		String[] ConditionColumnsValues011 = {"k3"};
		DBApp.select("enk", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((10%DBApp.dataPageSize))-1;
		DBApp.select("enk", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((10%DBApp.dataPageSize))-1;
		DBApp.select("enk", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"e"};
		String[] ConditionColumnsValues014 = {"e0"};
		DBApp.select("enk", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"h"};
		String[] ConditionColumnsValues015 = {"h1"};
		DBApp.select("enk", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((10%DBApp.dataPageSize))-1;
		DBApp.select("enk", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"j","a","c","k","h","o","m"};
		String[] ConditionColumnsValues017 = {"j7","a7","c1","k7","h7","o7","m7"};
		DBApp.select("enk", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("enk");
	
		String selectTrace0 = DBApp.getFullTrace("enk");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 31);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 10"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qvtz3", cols0);
		String [][] records_qvtz3 = new String[288][cols0.length];
		for(int i=0;i<288;i++)
		{
			records_qvtz3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qvtz3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qvtz3", records_qvtz3[i]);
		}
	
		int pageCount0 = (int)Math.ceil(288.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((288%DBApp.dataPageSize))-1;
		DBApp.select("qvtz3", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"g"};
		String[] ConditionColumnsValues01 = {"g4"};
		DBApp.select("qvtz3", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"n"};
		String[] ConditionColumnsValues02 = {"n4"};
		DBApp.select("qvtz3", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("qvtz3");
	
		String selectTrace0 = DBApp.getFullTrace("qvtz3");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 294);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 288"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p4", cols1);
		String [][] records_p4 = new String[161][cols1.length];
		for(int i=0;i<161;i++)
		{
			records_p4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p4", records_p4[i]);
		}
	
		int pageCount1 = (int)Math.ceil(161.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns10 = {"i","f","b"};
		String[] ConditionColumnsValues10 = {"i7","f4","b0"};
		DBApp.select("p4", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"d"};
		String[] ConditionColumnsValues11 = {"d1"};
		DBApp.select("p4", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("p4");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((161%DBApp.dataPageSize))-1;
		DBApp.select("p4", pageSelectPointer13,recordSelectPointer13);
	
		String selectTrace1 = DBApp.getFullTrace("p4");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 167);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 161"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("ag9nf", cols2);
		String [][] records_ag9nf = new String[132][cols2.length];
		for(int i=0;i<132;i++)
		{
			records_ag9nf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ag9nf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ag9nf", records_ag9nf[i]);
		}
	
		int pageCount2 = (int)Math.ceil(132.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns20 = {"c"};
		String[] ConditionColumnsValues20 = {"c0"};
		DBApp.select("ag9nf", ConditionColumns20, ConditionColumnsValues20);
	
		String selectTrace2 = DBApp.getFullTrace("ag9nf");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 135);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 132"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("uc", cols3);
		String [][] records_uc = new String[361][cols3.length];
		for(int i=0;i<361;i++)
		{
			records_uc[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_uc[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("uc", records_uc[i]);
		}
	
		int pageCount3 = (int)Math.ceil(361.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		String[] ConditionColumns30 = {"b"};
		String[] ConditionColumnsValues30 = {"b0"};
		DBApp.select("uc", ConditionColumns30, ConditionColumnsValues30);
	
		String[] ConditionColumns31 = {"b"};
		String[] ConditionColumnsValues31 = {"b1"};
		DBApp.select("uc", ConditionColumns31, ConditionColumnsValues31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((361%DBApp.dataPageSize))-1;
		DBApp.select("uc", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("uc");
	
		String[] ConditionColumns34 = {"b"};
		String[] ConditionColumnsValues34 = {"b0"};
		DBApp.select("uc", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((361%DBApp.dataPageSize))-1;
		DBApp.select("uc", pageSelectPointer35,recordSelectPointer35);
	
		DBApp.select("uc");
	
		DBApp.select("uc");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((361%DBApp.dataPageSize))-1;
		DBApp.select("uc", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("uc");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((361%DBApp.dataPageSize))-1;
		DBApp.select("uc", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("uc");
	
		String[] ConditionColumns312 = {"b"};
		String[] ConditionColumnsValues312 = {"b0"};
		DBApp.select("uc", ConditionColumns312, ConditionColumnsValues312);
	
		String selectTrace3 = DBApp.getFullTrace("uc");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 376);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 361"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("uao8t", cols0);
		String [][] records_uao8t = new String[416][cols0.length];
		for(int i=0;i<416;i++)
		{
			records_uao8t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uao8t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uao8t", records_uao8t[i]);
		}
	
		int pageCount0 = (int)Math.ceil(416.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((416%DBApp.dataPageSize))-1;
		DBApp.select("uao8t", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("uao8t");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((416%DBApp.dataPageSize))-1;
		DBApp.select("uao8t", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((416%DBApp.dataPageSize))-1;
		DBApp.select("uao8t", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"c"};
		String[] ConditionColumnsValues04 = {"c0"};
		DBApp.select("uao8t", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("uao8t");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 423);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 416"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b1", cols0);
		String [][] records_b1 = new String[29][cols0.length];
		for(int i=0;i<29;i++)
		{
			records_b1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b1", records_b1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(29.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((29%DBApp.dataPageSize))-1;
		DBApp.select("b1", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"c"};
		String[] ConditionColumnsValues01 = {"c0"};
		DBApp.select("b1", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((29%DBApp.dataPageSize))-1;
		DBApp.select("b1", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((29%DBApp.dataPageSize))-1;
		DBApp.select("b1", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("b1");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((29%DBApp.dataPageSize))-1;
		DBApp.select("b1", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((29%DBApp.dataPageSize))-1;
		DBApp.select("b1", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"b"};
		String[] ConditionColumnsValues07 = {"b0"};
		DBApp.select("b1", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("b1");
	
		String[] ConditionColumns09 = {"f","g"};
		String[] ConditionColumnsValues09 = {"f1","g5"};
		DBApp.select("b1", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"b","f","e"};
		String[] ConditionColumnsValues010 = {"b0","f4","e3"};
		DBApp.select("b1", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("b1");
	
		String[] ConditionColumns012 = {"c"};
		String[] ConditionColumnsValues012 = {"c2"};
		DBApp.select("b1", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"c"};
		String[] ConditionColumnsValues013 = {"c1"};
		DBApp.select("b1", ConditionColumns013, ConditionColumnsValues013);
	
		String selectTrace0 = DBApp.getFullTrace("b1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 45);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 29"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("s666k", cols1);
		String [][] records_s666k = new String[287][cols1.length];
		for(int i=0;i<287;i++)
		{
			records_s666k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s666k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s666k", records_s666k[i]);
		}
	
		int pageCount1 = (int)Math.ceil(287.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		DBApp.select("s666k");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("s666k");
	
		DBApp.select("s666k");
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b0"};
		DBApp.select("s666k", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"m"};
		String[] ConditionColumnsValues15 = {"m8"};
		DBApp.select("s666k", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"j","b","m"};
		String[] ConditionColumnsValues18 = {"j3","b1","m4"};
		DBApp.select("s666k", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"l"};
		String[] ConditionColumnsValues19 = {"l5"};
		DBApp.select("s666k", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"e"};
		String[] ConditionColumnsValues110 = {"e2"};
		DBApp.select("s666k", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer115,recordSelectPointer115);
	
		String[] ConditionColumns116 = {"m"};
		String[] ConditionColumnsValues116 = {"m3"};
		DBApp.select("s666k", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("s666k");
	
		String[] ConditionColumns118 = {"g"};
		String[] ConditionColumnsValues118 = {"g4"};
		DBApp.select("s666k", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("s666k");
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer121,recordSelectPointer121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer122,recordSelectPointer122);
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((287%DBApp.dataPageSize))-1;
		DBApp.select("s666k", pageSelectPointer123,recordSelectPointer123);
	
		String[] ConditionColumns124 = {"k"};
		String[] ConditionColumnsValues124 = {"k6"};
		DBApp.select("s666k", ConditionColumns124, ConditionColumnsValues124);
	
		String[] ConditionColumns125 = {"g"};
		String[] ConditionColumnsValues125 = {"g4"};
		DBApp.select("s666k", ConditionColumns125, ConditionColumnsValues125);
	
		String selectTrace1 = DBApp.getFullTrace("s666k");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 315);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 287"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("j615t", cols2);
		String [][] records_j615t = new String[456][cols2.length];
		for(int i=0;i<456;i++)
		{
			records_j615t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j615t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j615t", records_j615t[i]);
		}
	
		int pageCount2 = (int)Math.ceil(456.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("j615t");
	
		String[] ConditionColumns21 = {"a"};
		String[] ConditionColumnsValues21 = {"a218"};
		DBApp.select("j615t", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("j615t", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"c","e","f"};
		String[] ConditionColumnsValues23 = {"c0","e2","f0"};
		DBApp.select("j615t", ConditionColumns23, ConditionColumnsValues23);
	
		DBApp.select("j615t");
	
		String[] ConditionColumns25 = {"e","g"};
		String[] ConditionColumnsValues25 = {"e4","g2"};
		DBApp.select("j615t", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"c"};
		String[] ConditionColumnsValues26 = {"c0"};
		DBApp.select("j615t", ConditionColumns26, ConditionColumnsValues26);
	
		DBApp.select("j615t");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("j615t", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"f"};
		String[] ConditionColumnsValues29 = {"f2"};
		DBApp.select("j615t", ConditionColumns29, ConditionColumnsValues29);
	
		String[] ConditionColumns210 = {"g","e"};
		String[] ConditionColumnsValues210 = {"g1","e4"};
		DBApp.select("j615t", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("j615t", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("j615t", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("j615t");
	
		String[] ConditionColumns214 = {"c"};
		String[] ConditionColumnsValues214 = {"c0"};
		DBApp.select("j615t", ConditionColumns214, ConditionColumnsValues214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("j615t", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"d"};
		String[] ConditionColumnsValues216 = {"d3"};
		DBApp.select("j615t", ConditionColumns216, ConditionColumnsValues216);
	
		DBApp.select("j615t");
	
		DBApp.select("j615t");
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((456%DBApp.dataPageSize))-1;
		DBApp.select("j615t", pageSelectPointer219,recordSelectPointer219);
	
		String selectTrace2 = DBApp.getFullTrace("j615t");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 478);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 456"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("c2", cols3);
		String [][] records_c2 = new String[490][cols3.length];
		for(int i=0;i<490;i++)
		{
			records_c2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_c2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("c2", records_c2[i]);
		}
	
		int pageCount3 = (int)Math.ceil(490.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("c2");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("c2", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("c2", pageSelectPointer32,recordSelectPointer32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("c2", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("c2", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"n","a","g","k","b","c","m"};
		String[] ConditionColumnsValues35 = {"n12","a418","g5","k0","b0","c1","m2"};
		DBApp.select("c2", ConditionColumns35, ConditionColumnsValues35);
	
		String[] ConditionColumns36 = {"e"};
		String[] ConditionColumnsValues36 = {"e4"};
		DBApp.select("c2", ConditionColumns36, ConditionColumnsValues36);
	
		String[] ConditionColumns37 = {"f","g"};
		String[] ConditionColumnsValues37 = {"f5","g3"};
		DBApp.select("c2", ConditionColumns37, ConditionColumnsValues37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("c2", pageSelectPointer38,recordSelectPointer38);
	
		String[] ConditionColumns39 = {"m","k","h"};
		String[] ConditionColumnsValues39 = {"m0","k3","h3"};
		DBApp.select("c2", ConditionColumns39, ConditionColumnsValues39);
	
		DBApp.select("c2");
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((490%DBApp.dataPageSize))-1;
		DBApp.select("c2", pageSelectPointer311,recordSelectPointer311);
	
		String[] ConditionColumns312 = {"o"};
		String[] ConditionColumnsValues312 = {"o6"};
		DBApp.select("c2", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"n"};
		String[] ConditionColumnsValues313 = {"n12"};
		DBApp.select("c2", ConditionColumns313, ConditionColumnsValues313);
	
		String[] ConditionColumns314 = {"c","h","k"};
		String[] ConditionColumnsValues314 = {"c1","h1","k3"};
		DBApp.select("c2", ConditionColumns314, ConditionColumnsValues314);
	
		DBApp.select("c2");
	
		String selectTrace3 = DBApp.getFullTrace("c2");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 508);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 490"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("di2jd", cols4);
		String [][] records_di2jd = new String[496][cols4.length];
		for(int i=0;i<496;i++)
		{
			records_di2jd[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_di2jd[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("di2jd", records_di2jd[i]);
		}
	
		int pageCount4 = (int)Math.ceil(496.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("di2jd", pageSelectPointer40,recordSelectPointer40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((496%DBApp.dataPageSize))-1;
		DBApp.select("di2jd", pageSelectPointer41,recordSelectPointer41);
	
		DBApp.select("di2jd");
	
		String selectTrace4 = DBApp.getFullTrace("di2jd");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 501);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 496"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("a11", cols0);
		String [][] records_a11 = new String[127][cols0.length];
		for(int i=0;i<127;i++)
		{
			records_a11[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a11[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a11", records_a11[i]);
		}
	
		int pageCount0 = (int)Math.ceil(127.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		String[] ConditionColumns00 = {"b"};
		String[] ConditionColumnsValues00 = {"b1"};
		DBApp.select("a11", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((127%DBApp.dataPageSize))-1;
		DBApp.select("a11", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("a11");
	
		DBApp.select("a11");
	
		DBApp.select("a11");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((127%DBApp.dataPageSize))-1;
		DBApp.select("a11", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"b"};
		String[] ConditionColumnsValues06 = {"b0"};
		DBApp.select("a11", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"b"};
		String[] ConditionColumnsValues07 = {"b0"};
		DBApp.select("a11", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("a11");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((127%DBApp.dataPageSize))-1;
		DBApp.select("a11", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b1"};
		DBApp.select("a11", ConditionColumns010, ConditionColumnsValues010);
	
		String selectTrace0 = DBApp.getFullTrace("a11");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 140);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 127"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("wz8", cols1);
		String [][] records_wz8 = new String[194][cols1.length];
		for(int i=0;i<194;i++)
		{
			records_wz8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wz8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wz8", records_wz8[i]);
		}
	
		int pageCount1 = (int)Math.ceil(194.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((194%DBApp.dataPageSize))-1;
		DBApp.select("wz8", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((194%DBApp.dataPageSize))-1;
		DBApp.select("wz8", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"c"};
		String[] ConditionColumnsValues12 = {"c2"};
		DBApp.select("wz8", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a89"};
		DBApp.select("wz8", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("wz8");
	
		String[] ConditionColumns15 = {"b"};
		String[] ConditionColumnsValues15 = {"b0"};
		DBApp.select("wz8", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((194%DBApp.dataPageSize))-1;
		DBApp.select("wz8", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b0"};
		DBApp.select("wz8", ConditionColumns17, ConditionColumnsValues17);
	
		String selectTrace1 = DBApp.getFullTrace("wz8");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 204);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 194"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("y04", cols2);
		String [][] records_y04 = new String[201][cols2.length];
		for(int i=0;i<201;i++)
		{
			records_y04[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y04[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y04", records_y04[i]);
		}
	
		int pageCount2 = (int)Math.ceil(201.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		DBApp.select("y04");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((201%DBApp.dataPageSize))-1;
		DBApp.select("y04", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("y04");
	
		DBApp.select("y04");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((201%DBApp.dataPageSize))-1;
		DBApp.select("y04", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((201%DBApp.dataPageSize))-1;
		DBApp.select("y04", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"c"};
		String[] ConditionColumnsValues26 = {"c2"};
		DBApp.select("y04", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((201%DBApp.dataPageSize))-1;
		DBApp.select("y04", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"a"};
		String[] ConditionColumnsValues28 = {"a152"};
		DBApp.select("y04", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"c"};
		String[] ConditionColumnsValues29 = {"c0"};
		DBApp.select("y04", ConditionColumns29, ConditionColumnsValues29);
	
		String selectTrace2 = DBApp.getFullTrace("y04");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 213);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 201"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b"};
		DBApp.createTable("i5w56", cols3);
		String [][] records_i5w56 = new String[11][cols3.length];
		for(int i=0;i<11;i++)
		{
			records_i5w56[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_i5w56[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("i5w56", records_i5w56[i]);
		}
	
		int pageCount3 = (int)Math.ceil(11.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		DBApp.select("i5w56");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((11%DBApp.dataPageSize))-1;
		DBApp.select("i5w56", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"a"};
		String[] ConditionColumnsValues32 = {"a10"};
		DBApp.select("i5w56", ConditionColumns32, ConditionColumnsValues32);
	
		String selectTrace3 = DBApp.getFullTrace("i5w56");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 16);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 11"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gmw", cols0);
		String [][] records_gmw = new String[185][cols0.length];
		for(int i=0;i<185;i++)
		{
			records_gmw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_gmw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("gmw", records_gmw[i]);
		}
	
		int pageCount0 = (int)Math.ceil(185.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		String[] ConditionColumns00 = {"e"};
		String[] ConditionColumnsValues00 = {"e4"};
		DBApp.select("gmw", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("gmw");
	
		DBApp.select("gmw");
	
		String selectTrace0 = DBApp.getFullTrace("gmw");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 190);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 185"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p02i", cols1);
		String [][] records_p02i = new String[289][cols1.length];
		for(int i=0;i<289;i++)
		{
			records_p02i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p02i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p02i", records_p02i[i]);
		}
	
		int pageCount1 = (int)Math.ceil(289.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		DBApp.select("p02i");
	
		DBApp.select("p02i");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("p02i", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("p02i", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("p02i", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("p02i", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"f","g"};
		String[] ConditionColumnsValues16 = {"f4","g2"};
		DBApp.select("p02i", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"a"};
		String[] ConditionColumnsValues17 = {"a39"};
		DBApp.select("p02i", ConditionColumns17, ConditionColumnsValues17);
	
		String selectTrace1 = DBApp.getFullTrace("p02i");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 299);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 289"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("nlr", cols2);
		String [][] records_nlr = new String[300][cols2.length];
		for(int i=0;i<300;i++)
		{
			records_nlr[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nlr[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nlr", records_nlr[i]);
		}
	
		int pageCount2 = (int)Math.ceil(300.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("nlr");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("nlr", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"e","h","c"};
		String[] ConditionColumnsValues22 = {"e1","h7","c1"};
		DBApp.select("nlr", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("nlr");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("nlr", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("nlr", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((300%DBApp.dataPageSize))-1;
		DBApp.select("nlr", pageSelectPointer26,recordSelectPointer26);
	
		String selectTrace2 = DBApp.getFullTrace("nlr");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 309);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 300"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("hqsad", cols0);
		String [][] records_hqsad = new String[182][cols0.length];
		for(int i=0;i<182;i++)
		{
			records_hqsad[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hqsad[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hqsad", records_hqsad[i]);
		}
	
		int pageCount0 = (int)Math.ceil(182.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((182%DBApp.dataPageSize))-1;
		DBApp.select("hqsad", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((182%DBApp.dataPageSize))-1;
		DBApp.select("hqsad", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("hqsad");
	
		String[] ConditionColumns03 = {"f"};
		String[] ConditionColumnsValues03 = {"f5"};
		DBApp.select("hqsad", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"f","c"};
		String[] ConditionColumnsValues04 = {"f4","c1"};
		DBApp.select("hqsad", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("hqsad");
	
		DBApp.select("hqsad");
	
		String[] ConditionColumns07 = {"a","d"};
		String[] ConditionColumnsValues07 = {"a43","d3"};
		DBApp.select("hqsad", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((182%DBApp.dataPageSize))-1;
		DBApp.select("hqsad", pageSelectPointer08,recordSelectPointer08);
	
		String selectTrace0 = DBApp.getFullTrace("hqsad");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 193);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 182"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("mc", cols1);
		String [][] records_mc = new String[479][cols1.length];
		for(int i=0;i<479;i++)
		{
			records_mc[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mc[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mc", records_mc[i]);
		}
	
		int pageCount1 = (int)Math.ceil(479.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("mc");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("mc");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("mc");
	
		DBApp.select("mc");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("mc");
	
		String[] ConditionColumns19 = {"d"};
		String[] ConditionColumnsValues19 = {"d3"};
		DBApp.select("mc", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"c","i","m","k"};
		String[] ConditionColumnsValues110 = {"c2","i5","m7","k3"};
		DBApp.select("mc", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("mc");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("mc");
	
		String[] ConditionColumns114 = {"e","j"};
		String[] ConditionColumnsValues114 = {"e4","j4"};
		DBApp.select("mc", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("mc");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer117,recordSelectPointer117);
	
		String[] ConditionColumns118 = {"c","k"};
		String[] ConditionColumnsValues118 = {"c1","k6"};
		DBApp.select("mc", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"l","i","j"};
		String[] ConditionColumnsValues119 = {"l2","i8","j6"};
		DBApp.select("mc", ConditionColumns119, ConditionColumnsValues119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((479%DBApp.dataPageSize))-1;
		DBApp.select("mc", pageSelectPointer121,recordSelectPointer121);
	
		String selectTrace1 = DBApp.getFullTrace("mc");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 503);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 479"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("upt", cols2);
		String [][] records_upt = new String[68][cols2.length];
		for(int i=0;i<68;i++)
		{
			records_upt[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_upt[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("upt", records_upt[i]);
		}
	
		int pageCount2 = (int)Math.ceil(68.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		DBApp.select("upt");
	
		DBApp.select("upt");
	
		String[] ConditionColumns22 = {"a"};
		String[] ConditionColumnsValues22 = {"a60"};
		DBApp.select("upt", ConditionColumns22, ConditionColumnsValues22);
	
		String[] ConditionColumns23 = {"b"};
		String[] ConditionColumnsValues23 = {"b0"};
		DBApp.select("upt", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"a"};
		String[] ConditionColumnsValues24 = {"a60"};
		DBApp.select("upt", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((68%DBApp.dataPageSize))-1;
		DBApp.select("upt", pageSelectPointer25,recordSelectPointer25);
	
		String selectTrace2 = DBApp.getFullTrace("upt");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 76);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 68"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("f35x", cols3);
		String [][] records_f35x = new String[157][cols3.length];
		for(int i=0;i<157;i++)
		{
			records_f35x[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_f35x[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("f35x", records_f35x[i]);
		}
	
		int pageCount3 = (int)Math.ceil(157.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns30 = {"b"};
		String[] ConditionColumnsValues30 = {"b1"};
		DBApp.select("f35x", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("f35x");
	
		String[] ConditionColumns32 = {"f"};
		String[] ConditionColumnsValues32 = {"f5"};
		DBApp.select("f35x", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("f35x");
	
		String[] ConditionColumns34 = {"d"};
		String[] ConditionColumnsValues34 = {"d3"};
		DBApp.select("f35x", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer35,recordSelectPointer35);
	
		String[] ConditionColumns36 = {"b","e"};
		String[] ConditionColumnsValues36 = {"b1","e4"};
		DBApp.select("f35x", ConditionColumns36, ConditionColumnsValues36);
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer37,recordSelectPointer37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("f35x");
	
		DBApp.select("f35x");
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer311,recordSelectPointer311);
	
		String[] ConditionColumns312 = {"d","f","c"};
		String[] ConditionColumnsValues312 = {"d2","f4","c1"};
		DBApp.select("f35x", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"b"};
		String[] ConditionColumnsValues313 = {"b1"};
		DBApp.select("f35x", ConditionColumns313, ConditionColumnsValues313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer314,recordSelectPointer314);
	
		DBApp.select("f35x");
	
		String[] ConditionColumns316 = {"c"};
		String[] ConditionColumnsValues316 = {"c1"};
		DBApp.select("f35x", ConditionColumns316, ConditionColumnsValues316);
	
		DBApp.select("f35x");
	
		String[] ConditionColumns318 = {"f"};
		String[] ConditionColumnsValues318 = {"f5"};
		DBApp.select("f35x", ConditionColumns318, ConditionColumnsValues318);
	
		int pageSelectPointer319 = genRandNum(pageCount3)-1;
		int recordSelectPointer319 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer319==pageCount3-1)
			recordSelectPointer319 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer319,recordSelectPointer319);
	
		String[] ConditionColumns320 = {"e"};
		String[] ConditionColumnsValues320 = {"e1"};
		DBApp.select("f35x", ConditionColumns320, ConditionColumnsValues320);
	
		String[] ConditionColumns321 = {"c","f"};
		String[] ConditionColumnsValues321 = {"c1","f4"};
		DBApp.select("f35x", ConditionColumns321, ConditionColumnsValues321);
	
		String[] ConditionColumns322 = {"d"};
		String[] ConditionColumnsValues322 = {"d2"};
		DBApp.select("f35x", ConditionColumns322, ConditionColumnsValues322);
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer323,recordSelectPointer323);
	
		DBApp.select("f35x");
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((157%DBApp.dataPageSize))-1;
		DBApp.select("f35x", pageSelectPointer325,recordSelectPointer325);
	
		DBApp.select("f35x");
	
		String[] ConditionColumns327 = {"e"};
		String[] ConditionColumnsValues327 = {"e1"};
		DBApp.select("f35x", ConditionColumns327, ConditionColumnsValues327);
	
		String selectTrace3 = DBApp.getFullTrace("f35x");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 187);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 157"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("u370", cols0);
		String [][] records_u370 = new String[353][cols0.length];
		for(int i=0;i<353;i++)
		{
			records_u370[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u370[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u370", records_u370[i]);
		}
	
		int pageCount0 = (int)Math.ceil(353.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		DBApp.select("u370");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((353%DBApp.dataPageSize))-1;
		DBApp.select("u370", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("u370");
	
		String[] ConditionColumns03 = {"d"};
		String[] ConditionColumnsValues03 = {"d1"};
		DBApp.select("u370", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((353%DBApp.dataPageSize))-1;
		DBApp.select("u370", pageSelectPointer04,recordSelectPointer04);
	
		String selectTrace0 = DBApp.getFullTrace("u370");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 360);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 353"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("ygw9", cols1);
		String [][] records_ygw9 = new String[338][cols1.length];
		for(int i=0;i<338;i++)
		{
			records_ygw9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ygw9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ygw9", records_ygw9[i]);
		}
	
		int pageCount1 = (int)Math.ceil(338.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((338%DBApp.dataPageSize))-1;
		DBApp.select("ygw9", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("ygw9");
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b1"};
		DBApp.select("ygw9", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b0"};
		DBApp.select("ygw9", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((338%DBApp.dataPageSize))-1;
		DBApp.select("ygw9", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((338%DBApp.dataPageSize))-1;
		DBApp.select("ygw9", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("ygw9");
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b0"};
		DBApp.select("ygw9", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"b"};
		String[] ConditionColumnsValues18 = {"b0"};
		DBApp.select("ygw9", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b1"};
		DBApp.select("ygw9", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"a"};
		String[] ConditionColumnsValues110 = {"a102"};
		DBApp.select("ygw9", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("ygw9");
	
		String[] ConditionColumns112 = {"b"};
		String[] ConditionColumnsValues112 = {"b0"};
		DBApp.select("ygw9", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((338%DBApp.dataPageSize))-1;
		DBApp.select("ygw9", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a204"};
		DBApp.select("ygw9", ConditionColumns114, ConditionColumnsValues114);
	
		DBApp.select("ygw9");
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((338%DBApp.dataPageSize))-1;
		DBApp.select("ygw9", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("ygw9");
	
		DBApp.select("ygw9");
	
		DBApp.select("ygw9");
	
		String[] ConditionColumns120 = {"a"};
		String[] ConditionColumnsValues120 = {"a170"};
		DBApp.select("ygw9", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b1"};
		DBApp.select("ygw9", ConditionColumns121, ConditionColumnsValues121);
	
		DBApp.select("ygw9");
	
		String selectTrace1 = DBApp.getFullTrace("ygw9");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 363);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 338"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ivc", cols2);
		String [][] records_ivc = new String[372][cols2.length];
		for(int i=0;i<372;i++)
		{
			records_ivc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ivc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ivc", records_ivc[i]);
		}
	
		int pageCount2 = (int)Math.ceil(372.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		String[] ConditionColumns20 = {"d","l","k","c"};
		String[] ConditionColumnsValues20 = {"d2","l10","k10","c1"};
		DBApp.select("ivc", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"d","b","f","j"};
		String[] ConditionColumnsValues21 = {"d1","b1","f3","j3"};
		DBApp.select("ivc", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"b"};
		String[] ConditionColumnsValues22 = {"b0"};
		DBApp.select("ivc", ConditionColumns22, ConditionColumnsValues22);
	
		String[] ConditionColumns23 = {"g","d","c"};
		String[] ConditionColumnsValues23 = {"g2","d2","c0"};
		DBApp.select("ivc", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("ivc");
	
		String[] ConditionColumns27 = {"k"};
		String[] ConditionColumnsValues27 = {"k7"};
		DBApp.select("ivc", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"l"};
		String[] ConditionColumnsValues28 = {"l4"};
		DBApp.select("ivc", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("ivc");
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer212,recordSelectPointer212);
	
		String[] ConditionColumns213 = {"c"};
		String[] ConditionColumnsValues213 = {"c1"};
		DBApp.select("ivc", ConditionColumns213, ConditionColumnsValues213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"c"};
		String[] ConditionColumnsValues216 = {"c0"};
		DBApp.select("ivc", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"i"};
		String[] ConditionColumnsValues217 = {"i7"};
		DBApp.select("ivc", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("ivc");
	
		DBApp.select("ivc");
	
		String[] ConditionColumns220 = {"d"};
		String[] ConditionColumnsValues220 = {"d0"};
		DBApp.select("ivc", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"g"};
		String[] ConditionColumnsValues221 = {"g3"};
		DBApp.select("ivc", ConditionColumns221, ConditionColumnsValues221);
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer222,recordSelectPointer222);
	
		DBApp.select("ivc");
	
		String[] ConditionColumns224 = {"j","g","e"};
		String[] ConditionColumnsValues224 = {"j0","g6","e0"};
		DBApp.select("ivc", ConditionColumns224, ConditionColumnsValues224);
	
		String[] ConditionColumns225 = {"a","j"};
		String[] ConditionColumnsValues225 = {"a248","j8"};
		DBApp.select("ivc", ConditionColumns225, ConditionColumnsValues225);
	
		int pageSelectPointer226 = genRandNum(pageCount2)-1;
		int recordSelectPointer226 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer226==pageCount2-1)
			recordSelectPointer226 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("ivc", pageSelectPointer226,recordSelectPointer226);
	
		String selectTrace2 = DBApp.getFullTrace("ivc");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 401);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 372"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("w07", cols0);
		String [][] records_w07 = new String[398][cols0.length];
		for(int i=0;i<398;i++)
		{
			records_w07[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_w07[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("w07", records_w07[i]);
		}
	
		int pageCount0 = (int)Math.ceil(398.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		String[] ConditionColumns00 = {"b"};
		String[] ConditionColumnsValues00 = {"b1"};
		DBApp.select("w07", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b0"};
		DBApp.select("w07", ConditionColumns01, ConditionColumnsValues01);
	
		String selectTrace0 = DBApp.getFullTrace("w07");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 402);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 398"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("lra", cols0);
		String [][] records_lra = new String[243][cols0.length];
		for(int i=0;i<243;i++)
		{
			records_lra[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lra[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lra", records_lra[i]);
		}
	
		int pageCount0 = (int)Math.ceil(243.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((243%DBApp.dataPageSize))-1;
		DBApp.select("lra", pageSelectPointer00,recordSelectPointer00);
	
		String selectTrace0 = DBApp.getFullTrace("lra");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 246);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 243"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("i7", cols0);
		String [][] records_i7 = new String[497][cols0.length];
		for(int i=0;i<497;i++)
		{
			records_i7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i7", records_i7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(497.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		DBApp.select("i7");
	
		DBApp.select("i7");
	
		String[] ConditionColumns02 = {"c"};
		String[] ConditionColumnsValues02 = {"c0"};
		DBApp.select("i7", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"b","e"};
		String[] ConditionColumnsValues04 = {"b1","e2"};
		DBApp.select("i7", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("i7");
	
		String[] ConditionColumns06 = {"a"};
		String[] ConditionColumnsValues06 = {"a347"};
		DBApp.select("i7", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("i7");
	
		DBApp.select("i7");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("i7");
	
		DBApp.select("i7");
	
		String[] ConditionColumns012 = {"c"};
		String[] ConditionColumnsValues012 = {"c1"};
		DBApp.select("i7", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"a"};
		String[] ConditionColumnsValues013 = {"a119"};
		DBApp.select("i7", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("i7");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"c"};
		String[] ConditionColumnsValues017 = {"c1"};
		DBApp.select("i7", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("i7");
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer019,recordSelectPointer019);
	
		DBApp.select("i7");
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("i7");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((497%DBApp.dataPageSize))-1;
		DBApp.select("i7", pageSelectPointer024,recordSelectPointer024);
	
		DBApp.select("i7");
	
		DBApp.select("i7");
	
		String selectTrace0 = DBApp.getFullTrace("i7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 526);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 497"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("utq", cols0);
		String [][] records_utq = new String[438][cols0.length];
		for(int i=0;i<438;i++)
		{
			records_utq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_utq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("utq", records_utq[i]);
		}
	
		int pageCount0 = (int)Math.ceil(438.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((438%DBApp.dataPageSize))-1;
		DBApp.select("utq", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("utq");
	
		DBApp.select("utq");
	
		String[] ConditionColumns03 = {"b","l","k","c","f"};
		String[] ConditionColumnsValues03 = {"b0","l4","k4","c1","f4"};
		DBApp.select("utq", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"f","b","g","h","j"};
		String[] ConditionColumnsValues04 = {"f1","b1","g0","h1","j9"};
		DBApp.select("utq", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((438%DBApp.dataPageSize))-1;
		DBApp.select("utq", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((438%DBApp.dataPageSize))-1;
		DBApp.select("utq", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((438%DBApp.dataPageSize))-1;
		DBApp.select("utq", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((438%DBApp.dataPageSize))-1;
		DBApp.select("utq", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"k"};
		String[] ConditionColumnsValues09 = {"k0"};
		DBApp.select("utq", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"k","e","h","j","l","i"};
		String[] ConditionColumnsValues010 = {"k1","e1","h6","j6","l10","i4"};
		DBApp.select("utq", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("utq");
	
		String[] ConditionColumns012 = {"d","f","h","e"};
		String[] ConditionColumnsValues012 = {"d2","f0","h6","e1"};
		DBApp.select("utq", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"i"};
		String[] ConditionColumnsValues013 = {"i5"};
		DBApp.select("utq", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("utq");
	
		String[] ConditionColumns015 = {"f"};
		String[] ConditionColumnsValues015 = {"f4"};
		DBApp.select("utq", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"h"};
		String[] ConditionColumnsValues016 = {"h0"};
		DBApp.select("utq", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("utq");
	
		DBApp.select("utq");
	
		DBApp.select("utq");
	
		String selectTrace0 = DBApp.getFullTrace("utq");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 460);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 438"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("co8", cols1);
		String [][] records_co8 = new String[133][cols1.length];
		for(int i=0;i<133;i++)
		{
			records_co8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_co8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("co8", records_co8[i]);
		}
	
		int pageCount1 = (int)Math.ceil(133.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		DBApp.select("co8");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("co8", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("co8");
	
		DBApp.select("co8");
	
		DBApp.select("co8");
	
		String[] ConditionColumns15 = {"h"};
		String[] ConditionColumnsValues15 = {"h4"};
		DBApp.select("co8", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("co8");
	
		String[] ConditionColumns17 = {"c"};
		String[] ConditionColumnsValues17 = {"c1"};
		DBApp.select("co8", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"d"};
		String[] ConditionColumnsValues18 = {"d1"};
		DBApp.select("co8", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("co8", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("co8", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b1"};
		DBApp.select("co8", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"d","e","c"};
		String[] ConditionColumnsValues112 = {"d3","e0","c1"};
		DBApp.select("co8", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("co8");
	
		DBApp.select("co8");
	
		String[] ConditionColumns115 = {"d"};
		String[] ConditionColumnsValues115 = {"d3"};
		DBApp.select("co8", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("co8", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("co8");
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("co8", pageSelectPointer118,recordSelectPointer118);
	
		String[] ConditionColumns119 = {"c"};
		String[] ConditionColumnsValues119 = {"c0"};
		DBApp.select("co8", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("co8");
	
		DBApp.select("co8");
	
		DBApp.select("co8");
	
		String[] ConditionColumns123 = {"g"};
		String[] ConditionColumnsValues123 = {"g5"};
		DBApp.select("co8", ConditionColumns123, ConditionColumnsValues123);
	
		DBApp.select("co8");
	
		String[] ConditionColumns125 = {"a"};
		String[] ConditionColumnsValues125 = {"a70"};
		DBApp.select("co8", ConditionColumns125, ConditionColumnsValues125);
	
		String selectTrace1 = DBApp.getFullTrace("co8");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 161);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 133"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("cn4", cols2);
		String [][] records_cn4 = new String[169][cols2.length];
		for(int i=0;i<169;i++)
		{
			records_cn4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_cn4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("cn4", records_cn4[i]);
		}
	
		int pageCount2 = (int)Math.ceil(169.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		String[] ConditionColumns20 = {"b"};
		String[] ConditionColumnsValues20 = {"b1"};
		DBApp.select("cn4", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"b"};
		String[] ConditionColumnsValues21 = {"b0"};
		DBApp.select("cn4", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"a"};
		String[] ConditionColumnsValues22 = {"a93"};
		DBApp.select("cn4", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((169%DBApp.dataPageSize))-1;
		DBApp.select("cn4", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("cn4");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((169%DBApp.dataPageSize))-1;
		DBApp.select("cn4", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"b"};
		String[] ConditionColumnsValues26 = {"b1"};
		DBApp.select("cn4", ConditionColumns26, ConditionColumnsValues26);
	
		String selectTrace2 = DBApp.getFullTrace("cn4");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 178);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 169"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s51", cols3);
		String [][] records_s51 = new String[210][cols3.length];
		for(int i=0;i<210;i++)
		{
			records_s51[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_s51[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("s51", records_s51[i]);
		}
	
		int pageCount3 = (int)Math.ceil(210.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"f"};
		String[] ConditionColumnsValues31 = {"f1"};
		DBApp.select("s51", ConditionColumns31, ConditionColumnsValues31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer32,recordSelectPointer32);
	
		String[] ConditionColumns33 = {"f","b"};
		String[] ConditionColumnsValues33 = {"f2","b0"};
		DBApp.select("s51", ConditionColumns33, ConditionColumnsValues33);
	
		String[] ConditionColumns34 = {"c"};
		String[] ConditionColumnsValues34 = {"c2"};
		DBApp.select("s51", ConditionColumns34, ConditionColumnsValues34);
	
		String[] ConditionColumns35 = {"j"};
		String[] ConditionColumnsValues35 = {"j9"};
		DBApp.select("s51", ConditionColumns35, ConditionColumnsValues35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("s51");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("s51");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer310,recordSelectPointer310);
	
		String[] ConditionColumns311 = {"e"};
		String[] ConditionColumnsValues311 = {"e3"};
		DBApp.select("s51", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"i","d","h","g"};
		String[] ConditionColumnsValues312 = {"i4","d2","h2","g6"};
		DBApp.select("s51", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"j"};
		String[] ConditionColumnsValues313 = {"j2"};
		DBApp.select("s51", ConditionColumns313, ConditionColumnsValues313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer314,recordSelectPointer314);
	
		DBApp.select("s51");
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer316,recordSelectPointer316);
	
		DBApp.select("s51");
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer318,recordSelectPointer318);
	
		String[] ConditionColumns319 = {"h"};
		String[] ConditionColumnsValues319 = {"h2"};
		DBApp.select("s51", ConditionColumns319, ConditionColumnsValues319);
	
		String[] ConditionColumns320 = {"g"};
		String[] ConditionColumnsValues320 = {"g5"};
		DBApp.select("s51", ConditionColumns320, ConditionColumnsValues320);
	
		String[] ConditionColumns321 = {"h","d","g","b"};
		String[] ConditionColumnsValues321 = {"h7","d3","g5","b1"};
		DBApp.select("s51", ConditionColumns321, ConditionColumnsValues321);
	
		String[] ConditionColumns322 = {"b"};
		String[] ConditionColumnsValues322 = {"b1"};
		DBApp.select("s51", ConditionColumns322, ConditionColumnsValues322);
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer323,recordSelectPointer323);
	
		DBApp.select("s51");
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer325,recordSelectPointer325);
	
		DBApp.select("s51");
	
		int pageSelectPointer327 = genRandNum(pageCount3)-1;
		int recordSelectPointer327 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer327==pageCount3-1)
			recordSelectPointer327 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer327,recordSelectPointer327);
	
		int pageSelectPointer328 = genRandNum(pageCount3)-1;
		int recordSelectPointer328 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer328==pageCount3-1)
			recordSelectPointer328 = genRandNum((210%DBApp.dataPageSize))-1;
		DBApp.select("s51", pageSelectPointer328,recordSelectPointer328);
	
		DBApp.select("s51");
	
		String selectTrace3 = DBApp.getFullTrace("s51");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 242);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 210"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("n2q", cols0);
		String [][] records_n2q = new String[195][cols0.length];
		for(int i=0;i<195;i++)
		{
			records_n2q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n2q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n2q", records_n2q[i]);
		}
	
		int pageCount0 = (int)Math.ceil(195.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		DBApp.select("n2q");
	
		DBApp.select("n2q");
	
		String[] ConditionColumns02 = {"a","c","f"};
		String[] ConditionColumnsValues02 = {"a7","c1","f1"};
		DBApp.select("n2q", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("n2q", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"a","e","f"};
		String[] ConditionColumnsValues04 = {"a184","e4","f4"};
		DBApp.select("n2q", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("n2q", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"f"};
		String[] ConditionColumnsValues06 = {"f3"};
		DBApp.select("n2q", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("n2q");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("n2q", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("n2q");
	
		DBApp.select("n2q");
	
		String[] ConditionColumns011 = {"e","d","b"};
		String[] ConditionColumnsValues011 = {"e4","d0","b0"};
		DBApp.select("n2q", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"b"};
		String[] ConditionColumnsValues012 = {"b1"};
		DBApp.select("n2q", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("n2q");
	
		String[] ConditionColumns014 = {"d","b"};
		String[] ConditionColumnsValues014 = {"d0","b0"};
		DBApp.select("n2q", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("n2q");
	
		String[] ConditionColumns016 = {"b"};
		String[] ConditionColumnsValues016 = {"b1"};
		DBApp.select("n2q", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"b","c"};
		String[] ConditionColumnsValues017 = {"b0","c0"};
		DBApp.select("n2q", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("n2q", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"d"};
		String[] ConditionColumnsValues019 = {"d0"};
		DBApp.select("n2q", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("n2q");
	
		String selectTrace0 = DBApp.getFullTrace("n2q");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 218);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 195"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("v7", cols0);
		String [][] records_v7 = new String[150][cols0.length];
		for(int i=0;i<150;i++)
		{
			records_v7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v7", records_v7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(150.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("v7", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("v7");
	
		String[] ConditionColumns02 = {"g","f"};
		String[] ConditionColumnsValues02 = {"g1","f0"};
		DBApp.select("v7", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("v7");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("v7", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"f","h"};
		String[] ConditionColumnsValues05 = {"f2","h4"};
		DBApp.select("v7", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"b"};
		String[] ConditionColumnsValues06 = {"b0"};
		DBApp.select("v7", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("v7");
	
		DBApp.select("v7");
	
		DBApp.select("v7");
	
		String[] ConditionColumns010 = {"d"};
		String[] ConditionColumnsValues010 = {"d3"};
		DBApp.select("v7", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((150%DBApp.dataPageSize))-1;
		DBApp.select("v7", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("v7");
	
		DBApp.select("v7");
	
		String selectTrace0 = DBApp.getFullTrace("v7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 166);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 150"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o8", cols0);
		String [][] records_o8 = new String[199][cols0.length];
		for(int i=0;i<199;i++)
		{
			records_o8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o8", records_o8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(199.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns00 = {"g"};
		String[] ConditionColumnsValues00 = {"g2"};
		DBApp.select("o8", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"e","f"};
		String[] ConditionColumnsValues01 = {"e4","f5"};
		DBApp.select("o8", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("o8");
	
		DBApp.select("o8");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((199%DBApp.dataPageSize))-1;
		DBApp.select("o8", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("o8");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((199%DBApp.dataPageSize))-1;
		DBApp.select("o8", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"g"};
		String[] ConditionColumnsValues07 = {"g2"};
		DBApp.select("o8", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"c"};
		String[] ConditionColumnsValues08 = {"c1"};
		DBApp.select("o8", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"f"};
		String[] ConditionColumnsValues09 = {"f0"};
		DBApp.select("o8", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b1"};
		DBApp.select("o8", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((199%DBApp.dataPageSize))-1;
		DBApp.select("o8", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"e","g","a"};
		String[] ConditionColumnsValues012 = {"e1","g0","a126"};
		DBApp.select("o8", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d2"};
		DBApp.select("o8", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("o8");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((199%DBApp.dataPageSize))-1;
		DBApp.select("o8", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"b","e"};
		String[] ConditionColumnsValues016 = {"b1","e2"};
		DBApp.select("o8", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("o8");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((199%DBApp.dataPageSize))-1;
		DBApp.select("o8", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"c","e","g"};
		String[] ConditionColumnsValues019 = {"c1","e2","g4"};
		DBApp.select("o8", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"b"};
		String[] ConditionColumnsValues020 = {"b0"};
		DBApp.select("o8", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((199%DBApp.dataPageSize))-1;
		DBApp.select("o8", pageSelectPointer021,recordSelectPointer021);
	
		String selectTrace0 = DBApp.getFullTrace("o8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 223);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 199"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("kyh", cols1);
		String [][] records_kyh = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			records_kyh[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_kyh[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("kyh", records_kyh[i]);
		}
	
		int pageCount1 = (int)Math.ceil(3.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns10 = {"d"};
		String[] ConditionColumnsValues10 = {"d2"};
		DBApp.select("kyh", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("kyh");
	
		DBApp.select("kyh");
	
		String[] ConditionColumns13 = {"d"};
		String[] ConditionColumnsValues13 = {"d2"};
		DBApp.select("kyh", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("kyh");
	
		DBApp.select("kyh");
	
		String[] ConditionColumns16 = {"d"};
		String[] ConditionColumnsValues16 = {"d1"};
		DBApp.select("kyh", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"d"};
		String[] ConditionColumnsValues17 = {"d2"};
		DBApp.select("kyh", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"c"};
		String[] ConditionColumnsValues18 = {"c2"};
		DBApp.select("kyh", ConditionColumns18, ConditionColumnsValues18);
	
		String selectTrace1 = DBApp.getFullTrace("kyh");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 14);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 3"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("en", cols0);
		String [][] records_en = new String[195][cols0.length];
		for(int i=0;i<195;i++)
		{
			records_en[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_en[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("en", records_en[i]);
		}
	
		int pageCount0 = (int)Math.ceil(195.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("en");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("en");
	
		String[] ConditionColumns03 = {"g","b","c"};
		String[] ConditionColumnsValues03 = {"g5","b1","c1"};
		DBApp.select("en", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("en");
	
		DBApp.select("en");
	
		String[] ConditionColumns07 = {"e"};
		String[] ConditionColumnsValues07 = {"e4"};
		DBApp.select("en", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("en");
	
		String[] ConditionColumns09 = {"f"};
		String[] ConditionColumnsValues09 = {"f3"};
		DBApp.select("en", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"b","d","h"};
		String[] ConditionColumnsValues010 = {"b0","d2","h2"};
		DBApp.select("en", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("en");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("en");
	
		String[] ConditionColumns014 = {"c"};
		String[] ConditionColumnsValues014 = {"c2"};
		DBApp.select("en", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("en");
	
		DBApp.select("en");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"c"};
		String[] ConditionColumnsValues020 = {"c1"};
		DBApp.select("en", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((195%DBApp.dataPageSize))-1;
		DBApp.select("en", pageSelectPointer021,recordSelectPointer021);
	
		String[] ConditionColumns022 = {"b"};
		String[] ConditionColumnsValues022 = {"b1"};
		DBApp.select("en", ConditionColumns022, ConditionColumnsValues022);
	
		String selectTrace0 = DBApp.getFullTrace("en");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 220);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 195"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l6ly2", cols1);
		String [][] records_l6ly2 = new String[105][cols1.length];
		for(int i=0;i<105;i++)
		{
			records_l6ly2[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_l6ly2[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("l6ly2", records_l6ly2[i]);
		}
	
		int pageCount1 = (int)Math.ceil(105.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		String[] ConditionColumns10 = {"h"};
		String[] ConditionColumnsValues10 = {"h4"};
		DBApp.select("l6ly2", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"o"};
		String[] ConditionColumnsValues11 = {"o5"};
		DBApp.select("l6ly2", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"i","j","e","l","m"};
		String[] ConditionColumnsValues12 = {"i6","j1","e1","l3","m12"};
		DBApp.select("l6ly2", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("l6ly2", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"e","k","n","o"};
		String[] ConditionColumnsValues14 = {"e0","k10","n10","o10"};
		DBApp.select("l6ly2", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("l6ly2", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"c","l","i","d","e"};
		String[] ConditionColumnsValues16 = {"c2","l5","i5","d1","e0"};
		DBApp.select("l6ly2", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("l6ly2", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"l"};
		String[] ConditionColumnsValues18 = {"l4"};
		DBApp.select("l6ly2", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"d"};
		String[] ConditionColumnsValues19 = {"d0"};
		DBApp.select("l6ly2", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"i","l","c","k","b","j"};
		String[] ConditionColumnsValues110 = {"i6","l0","c0","k5","b0","j0"};
		DBApp.select("l6ly2", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("l6ly2");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("l6ly2", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("l6ly2", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"o"};
		String[] ConditionColumnsValues114 = {"o9"};
		DBApp.select("l6ly2", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((105%DBApp.dataPageSize))-1;
		DBApp.select("l6ly2", pageSelectPointer115,recordSelectPointer115);
	
		String selectTrace1 = DBApp.getFullTrace("l6ly2");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 123);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 105"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("q16", cols2);
		String [][] records_q16 = new String[185][cols2.length];
		for(int i=0;i<185;i++)
		{
			records_q16[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q16[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q16", records_q16[i]);
		}
	
		int pageCount2 = (int)Math.ceil(185.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		String[] ConditionColumns20 = {"a"};
		String[] ConditionColumnsValues20 = {"a16"};
		DBApp.select("q16", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("q16");
	
		String[] ConditionColumns22 = {"a"};
		String[] ConditionColumnsValues22 = {"a82"};
		DBApp.select("q16", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("q16");
	
		DBApp.select("q16");
	
		String[] ConditionColumns25 = {"a"};
		String[] ConditionColumnsValues25 = {"a151"};
		DBApp.select("q16", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("q16");
	
		String[] ConditionColumns27 = {"a"};
		String[] ConditionColumnsValues27 = {"a40"};
		DBApp.select("q16", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("q16");
	
		DBApp.select("q16");
	
		String[] ConditionColumns210 = {"a"};
		String[] ConditionColumnsValues210 = {"a31"};
		DBApp.select("q16", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("q16");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"a"};
		String[] ConditionColumnsValues214 = {"a101"};
		DBApp.select("q16", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"a"};
		String[] ConditionColumnsValues215 = {"a139"};
		DBApp.select("q16", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer216,recordSelectPointer216);
	
		DBApp.select("q16");
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer218,recordSelectPointer218);
	
		String[] ConditionColumns219 = {"a"};
		String[] ConditionColumnsValues219 = {"a90"};
		DBApp.select("q16", ConditionColumns219, ConditionColumnsValues219);
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer220,recordSelectPointer220);
	
		String[] ConditionColumns221 = {"a"};
		String[] ConditionColumnsValues221 = {"a134"};
		DBApp.select("q16", ConditionColumns221, ConditionColumnsValues221);
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer222,recordSelectPointer222);
	
		DBApp.select("q16");
	
		String[] ConditionColumns224 = {"a"};
		String[] ConditionColumnsValues224 = {"a6"};
		DBApp.select("q16", ConditionColumns224, ConditionColumnsValues224);
	
		DBApp.select("q16");
	
		int pageSelectPointer226 = genRandNum(pageCount2)-1;
		int recordSelectPointer226 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer226==pageCount2-1)
			recordSelectPointer226 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer226,recordSelectPointer226);
	
		String[] ConditionColumns227 = {"a"};
		String[] ConditionColumnsValues227 = {"a44"};
		DBApp.select("q16", ConditionColumns227, ConditionColumnsValues227);
	
		int pageSelectPointer228 = genRandNum(pageCount2)-1;
		int recordSelectPointer228 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer228==pageCount2-1)
			recordSelectPointer228 = genRandNum((185%DBApp.dataPageSize))-1;
		DBApp.select("q16", pageSelectPointer228,recordSelectPointer228);
	
		String[] ConditionColumns229 = {"a"};
		String[] ConditionColumnsValues229 = {"a117"};
		DBApp.select("q16", ConditionColumns229, ConditionColumnsValues229);
	
		String selectTrace2 = DBApp.getFullTrace("q16");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 217);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 185"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("x639", cols0);
		String [][] records_x639 = new String[358][cols0.length];
		for(int i=0;i<358;i++)
		{
			records_x639[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_x639[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("x639", records_x639[i]);
		}
	
		int pageCount0 = (int)Math.ceil(358.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e1"};
		DBApp.select("x639", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("x639");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("x639");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("x639");
	
		String[] ConditionColumns08 = {"a"};
		String[] ConditionColumnsValues08 = {"a62"};
		DBApp.select("x639", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"e","b"};
		String[] ConditionColumnsValues09 = {"e1","b0"};
		DBApp.select("x639", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("x639");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"c"};
		String[] ConditionColumnsValues012 = {"c0"};
		DBApp.select("x639", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"c"};
		String[] ConditionColumnsValues013 = {"c2"};
		DBApp.select("x639", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"e","c"};
		String[] ConditionColumnsValues015 = {"e0","c2"};
		DBApp.select("x639", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"e"};
		String[] ConditionColumnsValues016 = {"e0"};
		DBApp.select("x639", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("x639", pageSelectPointer017,recordSelectPointer017);
	
		DBApp.select("x639");
	
		String selectTrace0 = DBApp.getFullTrace("x639");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 379);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 358"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("c95", cols1);
		String [][] records_c95 = new String[119][cols1.length];
		for(int i=0;i<119;i++)
		{
			records_c95[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c95[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c95", records_c95[i]);
		}
	
		int pageCount1 = (int)Math.ceil(119.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("c95");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("c95");
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b0"};
		DBApp.select("c95", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("c95");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b1"};
		DBApp.select("c95", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b0"};
		DBApp.select("c95", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer111,recordSelectPointer111);
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a90"};
		DBApp.select("c95", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("c95");
	
		String[] ConditionColumns114 = {"b"};
		String[] ConditionColumnsValues114 = {"b1"};
		DBApp.select("c95", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"b"};
		String[] ConditionColumnsValues115 = {"b0"};
		DBApp.select("c95", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"b"};
		String[] ConditionColumnsValues116 = {"b1"};
		DBApp.select("c95", ConditionColumns116, ConditionColumnsValues116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("c95");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((119%DBApp.dataPageSize))-1;
		DBApp.select("c95", pageSelectPointer119,recordSelectPointer119);
	
		String selectTrace1 = DBApp.getFullTrace("c95");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 141);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 119"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ay1v3", cols2);
		String [][] records_ay1v3 = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			records_ay1v3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ay1v3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ay1v3", records_ay1v3[i]);
		}
	
		int pageCount2 = (int)Math.ceil(45.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns20 = {"k"};
		String[] ConditionColumnsValues20 = {"k4"};
		DBApp.select("ay1v3", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("ay1v3");
	
		DBApp.select("ay1v3");
	
		DBApp.select("ay1v3");
	
		String[] ConditionColumns24 = {"a","e","f","k","b"};
		String[] ConditionColumnsValues24 = {"a28","e3","f4","k6","b0"};
		DBApp.select("ay1v3", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"c","e","i","g"};
		String[] ConditionColumnsValues25 = {"c2","e3","i2","g3"};
		DBApp.select("ay1v3", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ay1v3", pageSelectPointer26,recordSelectPointer26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ay1v3", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ay1v3", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("ay1v3");
	
		DBApp.select("ay1v3");
	
		String[] ConditionColumns211 = {"f"};
		String[] ConditionColumnsValues211 = {"f4"};
		DBApp.select("ay1v3", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"j"};
		String[] ConditionColumnsValues212 = {"j4"};
		DBApp.select("ay1v3", ConditionColumns212, ConditionColumnsValues212);
	
		String[] ConditionColumns213 = {"c","e","f","d","k"};
		String[] ConditionColumnsValues213 = {"c1","e1","f1","d3","k9"};
		DBApp.select("ay1v3", ConditionColumns213, ConditionColumnsValues213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ay1v3", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"j","a","d","k"};
		String[] ConditionColumnsValues215 = {"j6","a26","d2","k4"};
		DBApp.select("ay1v3", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("ay1v3");
	
		DBApp.select("ay1v3");
	
		String[] ConditionColumns218 = {"j"};
		String[] ConditionColumnsValues218 = {"j1"};
		DBApp.select("ay1v3", ConditionColumns218, ConditionColumnsValues218);
	
		String[] ConditionColumns219 = {"c","k"};
		String[] ConditionColumnsValues219 = {"c0","k7"};
		DBApp.select("ay1v3", ConditionColumns219, ConditionColumnsValues219);
	
		DBApp.select("ay1v3");
	
		DBApp.select("ay1v3");
	
		String selectTrace2 = DBApp.getFullTrace("ay1v3");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 69);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 45"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("kl", cols0);
		String [][] records_kl = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			records_kl[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_kl[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("kl", records_kl[i]);
		}
	
		int pageCount0 = (int)Math.ceil(28.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("kl");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("kl");
	
		DBApp.select("kl");
	
		DBApp.select("kl");
	
		DBApp.select("kl");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b1"};
		DBApp.select("kl", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("kl");
	
		DBApp.select("kl");
	
		DBApp.select("kl");
	
		String[] ConditionColumns016 = {"d"};
		String[] ConditionColumnsValues016 = {"d0"};
		DBApp.select("kl", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("kl");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((28%DBApp.dataPageSize))-1;
		DBApp.select("kl", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"c"};
		String[] ConditionColumnsValues019 = {"c0"};
		DBApp.select("kl", ConditionColumns019, ConditionColumnsValues019);
	
		String selectTrace0 = DBApp.getFullTrace("kl");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 50);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 28"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("u717", cols1);
		String [][] records_u717 = new String[365][cols1.length];
		for(int i=0;i<365;i++)
		{
			records_u717[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u717[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u717", records_u717[i]);
		}
	
		int pageCount1 = (int)Math.ceil(365.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((365%DBApp.dataPageSize))-1;
		DBApp.select("u717", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b1"};
		DBApp.select("u717", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b1"};
		DBApp.select("u717", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((365%DBApp.dataPageSize))-1;
		DBApp.select("u717", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("u717");
	
		DBApp.select("u717");
	
		DBApp.select("u717");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((365%DBApp.dataPageSize))-1;
		DBApp.select("u717", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a281"};
		DBApp.select("u717", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"a"};
		String[] ConditionColumnsValues19 = {"a186"};
		DBApp.select("u717", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("u717");
	
		DBApp.select("u717");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((365%DBApp.dataPageSize))-1;
		DBApp.select("u717", pageSelectPointer112,recordSelectPointer112);
	
		String selectTrace1 = DBApp.getFullTrace("u717");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 380);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 365"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("mx", cols2);
		String [][] records_mx = new String[373][cols2.length];
		for(int i=0;i<373;i++)
		{
			records_mx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mx", records_mx[i]);
		}
	
		int pageCount2 = (int)Math.ceil(373.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("mx");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"j","h"};
		String[] ConditionColumnsValues22 = {"j4","h4"};
		DBApp.select("mx", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"i","b"};
		String[] ConditionColumnsValues25 = {"i3","b0"};
		DBApp.select("mx", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("mx");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer29,recordSelectPointer29);
	
		String[] ConditionColumns210 = {"i","g"};
		String[] ConditionColumnsValues210 = {"i8","g0"};
		DBApp.select("mx", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("mx");
	
		DBApp.select("mx");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("mx", pageSelectPointer214,recordSelectPointer214);
	
		DBApp.select("mx");
	
		DBApp.select("mx");
	
		String[] ConditionColumns217 = {"j","c","h"};
		String[] ConditionColumnsValues217 = {"j7","c0","h5"};
		DBApp.select("mx", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("mx");
	
		DBApp.select("mx");
	
		String selectTrace2 = DBApp.getFullTrace("mx");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 395);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 373"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("wwlef", cols3);
		String [][] records_wwlef = new String[493][cols3.length];
		for(int i=0;i<493;i++)
		{
			records_wwlef[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wwlef[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wwlef", records_wwlef[i]);
		}
	
		int pageCount3 = (int)Math.ceil(493.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		String[] ConditionColumns30 = {"e"};
		String[] ConditionColumnsValues30 = {"e2"};
		DBApp.select("wwlef", ConditionColumns30, ConditionColumnsValues30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((493%DBApp.dataPageSize))-1;
		DBApp.select("wwlef", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((493%DBApp.dataPageSize))-1;
		DBApp.select("wwlef", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("wwlef");
	
		DBApp.select("wwlef");
	
		String selectTrace3 = DBApp.getFullTrace("wwlef");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 500);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 493"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fto8", cols0);
		String [][] records_fto8 = new String[236][cols0.length];
		for(int i=0;i<236;i++)
		{
			records_fto8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fto8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fto8", records_fto8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(236.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"c"};
		String[] ConditionColumnsValues02 = {"c1"};
		DBApp.select("fto8", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("fto8");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("fto8");
	
		DBApp.select("fto8");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"d","h","f","k"};
		String[] ConditionColumnsValues09 = {"d3","h7","f3","k9"};
		DBApp.select("fto8", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("fto8");
	
		DBApp.select("fto8");
	
		String[] ConditionColumns012 = {"a","j","k","c"};
		String[] ConditionColumnsValues012 = {"a170","j0","k5","c2"};
		DBApp.select("fto8", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"a","j","g","c","f","h"};
		String[] ConditionColumnsValues013 = {"a226","j6","g2","c1","f4","h2"};
		DBApp.select("fto8", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer014,recordSelectPointer014);
	
		DBApp.select("fto8");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("fto8");
	
		DBApp.select("fto8");
	
		String[] ConditionColumns019 = {"l","d","k","a"};
		String[] ConditionColumnsValues019 = {"l0","d0","k7","a216"};
		DBApp.select("fto8", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"l","k"};
		String[] ConditionColumnsValues020 = {"l10","k9"};
		DBApp.select("fto8", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("fto8");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((236%DBApp.dataPageSize))-1;
		DBApp.select("fto8", pageSelectPointer022,recordSelectPointer022);
	
		DBApp.select("fto8");
	
		String[] ConditionColumns024 = {"f"};
		String[] ConditionColumnsValues024 = {"f2"};
		DBApp.select("fto8", ConditionColumns024, ConditionColumnsValues024);
	
		String[] ConditionColumns025 = {"b","k","l"};
		String[] ConditionColumnsValues025 = {"b1","k5","l3"};
		DBApp.select("fto8", ConditionColumns025, ConditionColumnsValues025);
	
		DBApp.select("fto8");
	
		DBApp.select("fto8");
	
		String[] ConditionColumns028 = {"c","g","a","d","b"};
		String[] ConditionColumnsValues028 = {"c2","g1","a29","d1","b1"};
		DBApp.select("fto8", ConditionColumns028, ConditionColumnsValues028);
	
		String[] ConditionColumns029 = {"f","k","g","a","c","e"};
		String[] ConditionColumnsValues029 = {"f3","k0","g5","a33","c0","e3"};
		DBApp.select("fto8", ConditionColumns029, ConditionColumnsValues029);
	
		String selectTrace0 = DBApp.getFullTrace("fto8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 268);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 236"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o9g", cols1);
		String [][] records_o9g = new String[380][cols1.length];
		for(int i=0;i<380;i++)
		{
			records_o9g[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o9g[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o9g", records_o9g[i]);
		}
	
		int pageCount1 = (int)Math.ceil(380.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		String[] ConditionColumns10 = {"j"};
		String[] ConditionColumnsValues10 = {"j1"};
		DBApp.select("o9g", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("o9g");
	
		String[] ConditionColumns12 = {"c","i"};
		String[] ConditionColumnsValues12 = {"c0","i6"};
		DBApp.select("o9g", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("o9g");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((380%DBApp.dataPageSize))-1;
		DBApp.select("o9g", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"g"};
		String[] ConditionColumnsValues15 = {"g1"};
		DBApp.select("o9g", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("o9g");
	
		String[] ConditionColumns17 = {"d","e","h"};
		String[] ConditionColumnsValues17 = {"d1","e3","h5"};
		DBApp.select("o9g", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((380%DBApp.dataPageSize))-1;
		DBApp.select("o9g", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((380%DBApp.dataPageSize))-1;
		DBApp.select("o9g", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((380%DBApp.dataPageSize))-1;
		DBApp.select("o9g", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("o9g");
	
		String[] ConditionColumns112 = {"i"};
		String[] ConditionColumnsValues112 = {"i3"};
		DBApp.select("o9g", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((380%DBApp.dataPageSize))-1;
		DBApp.select("o9g", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"g"};
		String[] ConditionColumnsValues114 = {"g6"};
		DBApp.select("o9g", ConditionColumns114, ConditionColumnsValues114);
	
		String selectTrace1 = DBApp.getFullTrace("o9g");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 397);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 380"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("m4", cols2);
		String [][] records_m4 = new String[242][cols2.length];
		for(int i=0;i<242;i++)
		{
			records_m4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m4", records_m4[i]);
		}
	
		int pageCount2 = (int)Math.ceil(242.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("m4");
	
		DBApp.select("m4");
	
		DBApp.select("m4");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"g"};
		String[] ConditionColumnsValues25 = {"g0"};
		DBApp.select("m4", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer26,recordSelectPointer26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"l"};
		String[] ConditionColumnsValues28 = {"l10"};
		DBApp.select("m4", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("m4");
	
		DBApp.select("m4");
	
		DBApp.select("m4");
	
		DBApp.select("m4");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"d"};
		String[] ConditionColumnsValues214 = {"d2"};
		DBApp.select("m4", ConditionColumns214, ConditionColumnsValues214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("m4");
	
		DBApp.select("m4");
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer218,recordSelectPointer218);
	
		String[] ConditionColumns219 = {"l"};
		String[] ConditionColumnsValues219 = {"l0"};
		DBApp.select("m4", ConditionColumns219, ConditionColumnsValues219);
	
		String[] ConditionColumns220 = {"g"};
		String[] ConditionColumnsValues220 = {"g3"};
		DBApp.select("m4", ConditionColumns220, ConditionColumnsValues220);
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((242%DBApp.dataPageSize))-1;
		DBApp.select("m4", pageSelectPointer221,recordSelectPointer221);
	
		String selectTrace2 = DBApp.getFullTrace("m4");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 266);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 242"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vzm", cols0);
		String [][] records_vzm = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_vzm[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vzm[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vzm", records_vzm[i]);
		}
	
		int pageCount0 = (int)Math.ceil(19.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("vzm", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b0"};
		DBApp.select("vzm", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("vzm");
	
		DBApp.select("vzm");
	
		String[] ConditionColumns04 = {"d","b"};
		String[] ConditionColumnsValues04 = {"d1","b1"};
		DBApp.select("vzm", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("vzm");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("vzm", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("vzm");
	
		DBApp.select("vzm");
	
		String selectTrace0 = DBApp.getFullTrace("vzm");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 30);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 19"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x36", cols1);
		String [][] records_x36 = new String[431][cols1.length];
		for(int i=0;i<431;i++)
		{
			records_x36[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_x36[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("x36", records_x36[i]);
		}
	
		int pageCount1 = (int)Math.ceil(431.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		DBApp.select("x36");
	
		String[] ConditionColumns11 = {"h"};
		String[] ConditionColumnsValues11 = {"h5"};
		DBApp.select("x36", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("x36");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"o"};
		String[] ConditionColumnsValues18 = {"o1"};
		DBApp.select("x36", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("x36");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("x36");
	
		DBApp.select("x36");
	
		DBApp.select("x36");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer115,recordSelectPointer115);
	
		String[] ConditionColumns116 = {"l"};
		String[] ConditionColumnsValues116 = {"l0"};
		DBApp.select("x36", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("x36");
	
		DBApp.select("x36");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"i","e","g"};
		String[] ConditionColumnsValues120 = {"i8","e2","g2"};
		DBApp.select("x36", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b1"};
		DBApp.select("x36", ConditionColumns121, ConditionColumnsValues121);
	
		DBApp.select("x36");
	
		String[] ConditionColumns123 = {"k","l","j","c","d"};
		String[] ConditionColumnsValues123 = {"k2","l9","j7","c0","d1"};
		DBApp.select("x36", ConditionColumns123, ConditionColumnsValues123);
	
		int pageSelectPointer124 = genRandNum(pageCount1)-1;
		int recordSelectPointer124 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer124==pageCount1-1)
			recordSelectPointer124 = genRandNum((431%DBApp.dataPageSize))-1;
		DBApp.select("x36", pageSelectPointer124,recordSelectPointer124);
	
		String[] ConditionColumns125 = {"c"};
		String[] ConditionColumnsValues125 = {"c1"};
		DBApp.select("x36", ConditionColumns125, ConditionColumnsValues125);
	
		String selectTrace1 = DBApp.getFullTrace("x36");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 459);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 431"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("q51v", cols2);
		String [][] records_q51v = new String[8][cols2.length];
		for(int i=0;i<8;i++)
		{
			records_q51v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_q51v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("q51v", records_q51v[i]);
		}
	
		int pageCount2 = (int)Math.ceil(8.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns20 = {"f"};
		String[] ConditionColumnsValues20 = {"f3"};
		DBApp.select("q51v", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("q51v");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"d"};
		String[] ConditionColumnsValues25 = {"d0"};
		DBApp.select("q51v", ConditionColumns25, ConditionColumnsValues25);
	
		DBApp.select("q51v");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"f"};
		String[] ConditionColumnsValues28 = {"f5"};
		DBApp.select("q51v", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"d","h","a"};
		String[] ConditionColumnsValues29 = {"d1","h5","a5"};
		DBApp.select("q51v", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("q51v");
	
		DBApp.select("q51v");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("q51v");
	
		String[] ConditionColumns214 = {"d","j","k"};
		String[] ConditionColumnsValues214 = {"d0","j4","k4"};
		DBApp.select("q51v", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"h","f"};
		String[] ConditionColumnsValues215 = {"h5","f5"};
		DBApp.select("q51v", ConditionColumns215, ConditionColumnsValues215);
	
		String[] ConditionColumns216 = {"j","c","i"};
		String[] ConditionColumnsValues216 = {"j6","c0","i6"};
		DBApp.select("q51v", ConditionColumns216, ConditionColumnsValues216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer217,recordSelectPointer217);
	
		DBApp.select("q51v");
	
		String[] ConditionColumns219 = {"f","c"};
		String[] ConditionColumnsValues219 = {"f5","c2"};
		DBApp.select("q51v", ConditionColumns219, ConditionColumnsValues219);
	
		String[] ConditionColumns220 = {"a"};
		String[] ConditionColumnsValues220 = {"a1"};
		DBApp.select("q51v", ConditionColumns220, ConditionColumnsValues220);
	
		DBApp.select("q51v");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer222,recordSelectPointer222);
	
		DBApp.select("q51v");
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer224,recordSelectPointer224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer225,recordSelectPointer225);
	
		int pageSelectPointer226 = genRandNum(pageCount2)-1;
		int recordSelectPointer226 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer226==pageCount2-1)
			recordSelectPointer226 = genRandNum((8%DBApp.dataPageSize))-1;
		DBApp.select("q51v", pageSelectPointer226,recordSelectPointer226);
	
		String[] ConditionColumns227 = {"c"};
		String[] ConditionColumnsValues227 = {"c0"};
		DBApp.select("q51v", ConditionColumns227, ConditionColumnsValues227);
	
		DBApp.select("q51v");
	
		String selectTrace2 = DBApp.getFullTrace("q51v");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 39);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 8"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("kkk3", cols3);
		String [][] records_kkk3 = new String[61][cols3.length];
		for(int i=0;i<61;i++)
		{
			records_kkk3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_kkk3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("kkk3", records_kkk3[i]);
		}
	
		int pageCount3 = (int)Math.ceil(61.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer31,recordSelectPointer31);
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("kkk3");
	
		DBApp.select("kkk3");
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer35,recordSelectPointer35);
	
		String[] ConditionColumns36 = {"b"};
		String[] ConditionColumnsValues36 = {"b1"};
		DBApp.select("kkk3", ConditionColumns36, ConditionColumnsValues36);
	
		String[] ConditionColumns37 = {"b"};
		String[] ConditionColumnsValues37 = {"b0"};
		DBApp.select("kkk3", ConditionColumns37, ConditionColumnsValues37);
	
		String[] ConditionColumns38 = {"c"};
		String[] ConditionColumnsValues38 = {"c1"};
		DBApp.select("kkk3", ConditionColumns38, ConditionColumnsValues38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("kkk3");
	
		String[] ConditionColumns311 = {"c"};
		String[] ConditionColumnsValues311 = {"c0"};
		DBApp.select("kkk3", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"b"};
		String[] ConditionColumnsValues312 = {"b1"};
		DBApp.select("kkk3", ConditionColumns312, ConditionColumnsValues312);
	
		DBApp.select("kkk3");
	
		String[] ConditionColumns314 = {"b"};
		String[] ConditionColumnsValues314 = {"b1"};
		DBApp.select("kkk3", ConditionColumns314, ConditionColumnsValues314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer315,recordSelectPointer315);
	
		String[] ConditionColumns316 = {"a"};
		String[] ConditionColumnsValues316 = {"a39"};
		DBApp.select("kkk3", ConditionColumns316, ConditionColumnsValues316);
	
		String[] ConditionColumns317 = {"c"};
		String[] ConditionColumnsValues317 = {"c2"};
		DBApp.select("kkk3", ConditionColumns317, ConditionColumnsValues317);
	
		DBApp.select("kkk3");
	
		String[] ConditionColumns319 = {"b"};
		String[] ConditionColumnsValues319 = {"b1"};
		DBApp.select("kkk3", ConditionColumns319, ConditionColumnsValues319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer320,recordSelectPointer320);
	
		int pageSelectPointer321 = genRandNum(pageCount3)-1;
		int recordSelectPointer321 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer321==pageCount3-1)
			recordSelectPointer321 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer321,recordSelectPointer321);
	
		DBApp.select("kkk3");
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer323,recordSelectPointer323);
	
		int pageSelectPointer324 = genRandNum(pageCount3)-1;
		int recordSelectPointer324 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer324==pageCount3-1)
			recordSelectPointer324 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer324,recordSelectPointer324);
	
		DBApp.select("kkk3");
	
		int pageSelectPointer326 = genRandNum(pageCount3)-1;
		int recordSelectPointer326 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer326==pageCount3-1)
			recordSelectPointer326 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("kkk3", pageSelectPointer326,recordSelectPointer326);
	
		String[] ConditionColumns327 = {"c"};
		String[] ConditionColumnsValues327 = {"c0"};
		DBApp.select("kkk3", ConditionColumns327, ConditionColumnsValues327);
	
		String selectTrace3 = DBApp.getFullTrace("kkk3");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 91);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 61"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("dqc", cols0);
		String [][] records_dqc = new String[184][cols0.length];
		for(int i=0;i<184;i++)
		{
			records_dqc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dqc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dqc", records_dqc[i]);
		}
	
		int pageCount0 = (int)Math.ceil(184.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("dqc");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"b","g","c"};
		String[] ConditionColumnsValues05 = {"b1","g2","c0"};
		DBApp.select("dqc", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("dqc");
	
		DBApp.select("dqc");
	
		String[] ConditionColumns09 = {"b"};
		String[] ConditionColumnsValues09 = {"b1"};
		DBApp.select("dqc", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("dqc");
	
		DBApp.select("dqc");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"g","b"};
		String[] ConditionColumnsValues014 = {"g2","b1"};
		DBApp.select("dqc", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("dqc");
	
		String[] ConditionColumns016 = {"g"};
		String[] ConditionColumnsValues016 = {"g3"};
		DBApp.select("dqc", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"g"};
		String[] ConditionColumnsValues017 = {"g3"};
		DBApp.select("dqc", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("dqc");
	
		String[] ConditionColumns019 = {"g"};
		String[] ConditionColumnsValues019 = {"g6"};
		DBApp.select("dqc", ConditionColumns019, ConditionColumnsValues019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer020,recordSelectPointer020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer021,recordSelectPointer021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer022,recordSelectPointer022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer024,recordSelectPointer024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((184%DBApp.dataPageSize))-1;
		DBApp.select("dqc", pageSelectPointer025,recordSelectPointer025);
	
		DBApp.select("dqc");
	
		String selectTrace0 = DBApp.getFullTrace("dqc");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 213);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 184"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("y0j", cols0);
		String [][] records_y0j = new String[265][cols0.length];
		for(int i=0;i<265;i++)
		{
			records_y0j[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y0j[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y0j", records_y0j[i]);
		}
	
		int pageCount0 = (int)Math.ceil(265.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns00 = {"c"};
		String[] ConditionColumnsValues00 = {"c1"};
		DBApp.select("y0j", ConditionColumns00, ConditionColumnsValues00);
	
		String selectTrace0 = DBApp.getFullTrace("y0j");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 268);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 265"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("idzp", cols1);
		String [][] records_idzp = new String[363][cols1.length];
		for(int i=0;i<363;i++)
		{
			records_idzp[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_idzp[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("idzp", records_idzp[i]);
		}
	
		int pageCount1 = (int)Math.ceil(363.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		DBApp.select("idzp");
	
		String[] ConditionColumns11 = {"d"};
		String[] ConditionColumnsValues11 = {"d1"};
		DBApp.select("idzp", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b1"};
		DBApp.select("idzp", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"c","d"};
		String[] ConditionColumnsValues14 = {"c1","d0"};
		DBApp.select("idzp", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("idzp");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("idzp");
	
		String[] ConditionColumns19 = {"c"};
		String[] ConditionColumnsValues19 = {"c2"};
		DBApp.select("idzp", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"d"};
		String[] ConditionColumnsValues110 = {"d1"};
		DBApp.select("idzp", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a205"};
		DBApp.select("idzp", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"d"};
		String[] ConditionColumnsValues115 = {"d1"};
		DBApp.select("idzp", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((363%DBApp.dataPageSize))-1;
		DBApp.select("idzp", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("idzp");
	
		String[] ConditionColumns118 = {"d"};
		String[] ConditionColumnsValues118 = {"d3"};
		DBApp.select("idzp", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"b"};
		String[] ConditionColumnsValues119 = {"b0"};
		DBApp.select("idzp", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("idzp");
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b1"};
		DBApp.select("idzp", ConditionColumns121, ConditionColumnsValues121);
	
		DBApp.select("idzp");
	
		String selectTrace1 = DBApp.getFullTrace("idzp");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 388);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 363"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("la1", cols2);
		String [][] records_la1 = new String[133][cols2.length];
		for(int i=0;i<133;i++)
		{
			records_la1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_la1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("la1", records_la1[i]);
		}
	
		int pageCount2 = (int)Math.ceil(133.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns20 = {"f"};
		String[] ConditionColumnsValues20 = {"f3"};
		DBApp.select("la1", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("la1");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("la1", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("la1");
	
		String[] ConditionColumns24 = {"h","d","a"};
		String[] ConditionColumnsValues24 = {"h7","d3","a71"};
		DBApp.select("la1", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("la1");
	
		String[] ConditionColumns26 = {"d","f","i","h","j"};
		String[] ConditionColumnsValues26 = {"d2","f2","i5","h2","j0"};
		DBApp.select("la1", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("la1", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("la1");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("la1", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("la1");
	
		String[] ConditionColumns211 = {"h"};
		String[] ConditionColumnsValues211 = {"h4"};
		DBApp.select("la1", ConditionColumns211, ConditionColumnsValues211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("la1", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("la1", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("la1");
	
		String[] ConditionColumns215 = {"a","b","c","h","f"};
		String[] ConditionColumnsValues215 = {"a81","b1","c0","h1","f3"};
		DBApp.select("la1", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("la1");
	
		String[] ConditionColumns217 = {"e"};
		String[] ConditionColumnsValues217 = {"e2"};
		DBApp.select("la1", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("la1");
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("la1", pageSelectPointer219,recordSelectPointer219);
	
		DBApp.select("la1");
	
		String selectTrace2 = DBApp.getFullTrace("la1");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 156);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 133"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b8z", cols0);
		String [][] records_b8z = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			records_b8z[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_b8z[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("b8z", records_b8z[i]);
		}
	
		int pageCount0 = (int)Math.ceil(61.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("b8z");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"f"};
		String[] ConditionColumnsValues02 = {"f0"};
		DBApp.select("b8z", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"n","f","o","l"};
		String[] ConditionColumnsValues04 = {"n0","f2","o11","l8"};
		DBApp.select("b8z", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"o"};
		String[] ConditionColumnsValues05 = {"o12"};
		DBApp.select("b8z", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("b8z");
	
		String[] ConditionColumns07 = {"h","b"};
		String[] ConditionColumnsValues07 = {"h3","b1"};
		DBApp.select("b8z", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"n","m","l","k","j","o"};
		String[] ConditionColumnsValues08 = {"n2","m5","l8","k0","j4","o14"};
		DBApp.select("b8z", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"j","i","o","h","a"};
		String[] ConditionColumnsValues09 = {"j9","i3","o9","h7","a39"};
		DBApp.select("b8z", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("b8z");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("b8z");
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("b8z");
	
		String[] ConditionColumns015 = {"i"};
		String[] ConditionColumnsValues015 = {"i0"};
		DBApp.select("b8z", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer016,recordSelectPointer016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer018,recordSelectPointer018);
	
		DBApp.select("b8z");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer020,recordSelectPointer020);
	
		String[] ConditionColumns021 = {"f","k","h","g"};
		String[] ConditionColumnsValues021 = {"f0","k1","h4","g5"};
		DBApp.select("b8z", ConditionColumns021, ConditionColumnsValues021);
	
		DBApp.select("b8z");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer024,recordSelectPointer024);
	
		DBApp.select("b8z");
	
		String[] ConditionColumns026 = {"k","h","o","f","i","c","l"};
		String[] ConditionColumnsValues026 = {"k2","h1","o12","f3","i3","c0","l9"};
		DBApp.select("b8z", ConditionColumns026, ConditionColumnsValues026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((61%DBApp.dataPageSize))-1;
		DBApp.select("b8z", pageSelectPointer027,recordSelectPointer027);
	
		String[] ConditionColumns028 = {"l"};
		String[] ConditionColumnsValues028 = {"l4"};
		DBApp.select("b8z", ConditionColumns028, ConditionColumnsValues028);
	
		String selectTrace0 = DBApp.getFullTrace("b8z");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 92);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 61"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xbk9", cols1);
		String [][] records_xbk9 = new String[448][cols1.length];
		for(int i=0;i<448;i++)
		{
			records_xbk9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xbk9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xbk9", records_xbk9[i]);
		}
	
		int pageCount1 = (int)Math.ceil(448.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((448%DBApp.dataPageSize))-1;
		DBApp.select("xbk9", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"b","c"};
		String[] ConditionColumnsValues11 = {"b1","c1"};
		DBApp.select("xbk9", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((448%DBApp.dataPageSize))-1;
		DBApp.select("xbk9", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"g","e","b"};
		String[] ConditionColumnsValues13 = {"g2","e3","b0"};
		DBApp.select("xbk9", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c","g","f"};
		String[] ConditionColumnsValues14 = {"c1","g1","f1"};
		DBApp.select("xbk9", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"a"};
		String[] ConditionColumnsValues15 = {"a271"};
		DBApp.select("xbk9", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((448%DBApp.dataPageSize))-1;
		DBApp.select("xbk9", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"e"};
		String[] ConditionColumnsValues17 = {"e0"};
		DBApp.select("xbk9", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("xbk9");
	
		String[] ConditionColumns19 = {"e"};
		String[] ConditionColumnsValues19 = {"e1"};
		DBApp.select("xbk9", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"d"};
		String[] ConditionColumnsValues110 = {"d0"};
		DBApp.select("xbk9", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"g"};
		String[] ConditionColumnsValues111 = {"g6"};
		DBApp.select("xbk9", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("xbk9");
	
		String[] ConditionColumns113 = {"f"};
		String[] ConditionColumnsValues113 = {"f4"};
		DBApp.select("xbk9", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("xbk9");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((448%DBApp.dataPageSize))-1;
		DBApp.select("xbk9", pageSelectPointer115,recordSelectPointer115);
	
		String[] ConditionColumns116 = {"f"};
		String[] ConditionColumnsValues116 = {"f1"};
		DBApp.select("xbk9", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("xbk9");
	
		String[] ConditionColumns118 = {"f","d"};
		String[] ConditionColumnsValues118 = {"f3","d3"};
		DBApp.select("xbk9", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("xbk9");
	
		DBApp.select("xbk9");
	
		DBApp.select("xbk9");
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((448%DBApp.dataPageSize))-1;
		DBApp.select("xbk9", pageSelectPointer122,recordSelectPointer122);
	
		DBApp.select("xbk9");
	
		DBApp.select("xbk9");
	
		String[] ConditionColumns125 = {"d"};
		String[] ConditionColumnsValues125 = {"d3"};
		DBApp.select("xbk9", ConditionColumns125, ConditionColumnsValues125);
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((448%DBApp.dataPageSize))-1;
		DBApp.select("xbk9", pageSelectPointer126,recordSelectPointer126);
	
		String[] ConditionColumns127 = {"b","g"};
		String[] ConditionColumnsValues127 = {"b0","g1"};
		DBApp.select("xbk9", ConditionColumns127, ConditionColumnsValues127);
	
		String selectTrace1 = DBApp.getFullTrace("xbk9");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 478);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 448"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("s4", cols0);
		String [][] records_s4 = new String[124][cols0.length];
		for(int i=0;i<124;i++)
		{
			records_s4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s4", records_s4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(124.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("s4");
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b1"};
		DBApp.select("s4", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"e"};
		String[] ConditionColumnsValues04 = {"e1"};
		DBApp.select("s4", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"f"};
		String[] ConditionColumnsValues06 = {"f4"};
		DBApp.select("s4", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("s4");
	
		String[] ConditionColumns08 = {"c","b"};
		String[] ConditionColumnsValues08 = {"c2","b1"};
		DBApp.select("s4", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"c"};
		String[] ConditionColumnsValues09 = {"c2"};
		DBApp.select("s4", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"e"};
		String[] ConditionColumnsValues010 = {"e3"};
		DBApp.select("s4", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"e"};
		String[] ConditionColumnsValues013 = {"e4"};
		DBApp.select("s4", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"d"};
		String[] ConditionColumnsValues014 = {"d3"};
		DBApp.select("s4", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("s4");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"b"};
		String[] ConditionColumnsValues018 = {"b1"};
		DBApp.select("s4", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"b"};
		String[] ConditionColumnsValues020 = {"b1"};
		DBApp.select("s4", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("s4");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((124%DBApp.dataPageSize))-1;
		DBApp.select("s4", pageSelectPointer022,recordSelectPointer022);
	
		DBApp.select("s4");
	
		String selectTrace0 = DBApp.getFullTrace("s4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 150);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 124"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("yi", cols1);
		String [][] records_yi = new String[162][cols1.length];
		for(int i=0;i<162;i++)
		{
			records_yi[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yi[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yi", records_yi[i]);
		}
	
		int pageCount1 = (int)Math.ceil(162.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((162%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		String[] ConditionColumns13 = {"e"};
		String[] ConditionColumnsValues13 = {"e1"};
		DBApp.select("yi", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c0"};
		DBApp.select("yi", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		String[] ConditionColumns17 = {"c"};
		String[] ConditionColumnsValues17 = {"c1"};
		DBApp.select("yi", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((162%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((162%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"e"};
		String[] ConditionColumnsValues110 = {"e1"};
		DBApp.select("yi", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("yi");
	
		String[] ConditionColumns112 = {"d"};
		String[] ConditionColumnsValues112 = {"d3"};
		DBApp.select("yi", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		String[] ConditionColumns115 = {"d"};
		String[] ConditionColumnsValues115 = {"d0"};
		DBApp.select("yi", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("yi");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((162%DBApp.dataPageSize))-1;
		DBApp.select("yi", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("yi");
	
		DBApp.select("yi");
	
		String selectTrace1 = DBApp.getFullTrace("yi");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 184);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 162"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("lt0", cols0);
		String [][] records_lt0 = new String[324][cols0.length];
		for(int i=0;i<324;i++)
		{
			records_lt0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lt0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lt0", records_lt0[i]);
		}
	
		int pageCount0 = (int)Math.ceil(324.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		DBApp.select("lt0");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((324%DBApp.dataPageSize))-1;
		DBApp.select("lt0", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((324%DBApp.dataPageSize))-1;
		DBApp.select("lt0", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((324%DBApp.dataPageSize))-1;
		DBApp.select("lt0", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((324%DBApp.dataPageSize))-1;
		DBApp.select("lt0", pageSelectPointer04,recordSelectPointer04);
	
		String selectTrace0 = DBApp.getFullTrace("lt0");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 331);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 324"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jmb", cols0);
		String [][] records_jmb = new String[368][cols0.length];
		for(int i=0;i<368;i++)
		{
			records_jmb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jmb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jmb", records_jmb[i]);
		}
	
		int pageCount0 = (int)Math.ceil(368.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("jmb");
	
		DBApp.select("jmb");
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c0"};
		DBApp.select("jmb", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("jmb");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("jmb", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"e","d"};
		String[] ConditionColumnsValues011 = {"e0","d2"};
		DBApp.select("jmb", ConditionColumns011, ConditionColumnsValues011);
	
		String selectTrace0 = DBApp.getFullTrace("jmb");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 382);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 368"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("dc2ms", cols1);
		String [][] records_dc2ms = new String[222][cols1.length];
		for(int i=0;i<222;i++)
		{
			records_dc2ms[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dc2ms[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dc2ms", records_dc2ms[i]);
		}
	
		int pageCount1 = (int)Math.ceil(222.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((222%DBApp.dataPageSize))-1;
		DBApp.select("dc2ms", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"b"};
		String[] ConditionColumnsValues11 = {"b1"};
		DBApp.select("dc2ms", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((222%DBApp.dataPageSize))-1;
		DBApp.select("dc2ms", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((222%DBApp.dataPageSize))-1;
		DBApp.select("dc2ms", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((222%DBApp.dataPageSize))-1;
		DBApp.select("dc2ms", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("dc2ms");
	
		String[] ConditionColumns16 = {"g","d","b"};
		String[] ConditionColumnsValues16 = {"g3","d0","b0"};
		DBApp.select("dc2ms", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("dc2ms");
	
		DBApp.select("dc2ms");
	
		String[] ConditionColumns19 = {"g"};
		String[] ConditionColumnsValues19 = {"g6"};
		DBApp.select("dc2ms", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((222%DBApp.dataPageSize))-1;
		DBApp.select("dc2ms", pageSelectPointer110,recordSelectPointer110);
	
		String selectTrace1 = DBApp.getFullTrace("dc2ms");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 235);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 222"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vo", cols0);
		String [][] records_vo = new String[251][cols0.length];
		for(int i=0;i<251;i++)
		{
			records_vo[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vo[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vo", records_vo[i]);
		}
	
		int pageCount0 = (int)Math.ceil(251.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"k"};
		String[] ConditionColumnsValues01 = {"k3"};
		DBApp.select("vo", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a244"};
		DBApp.select("vo", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("vo");
	
		DBApp.select("vo");
	
		String[] ConditionColumns07 = {"j","a","f"};
		String[] ConditionColumnsValues07 = {"j0","a110","f2"};
		DBApp.select("vo", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("vo");
	
		String[] ConditionColumns011 = {"e","j","b"};
		String[] ConditionColumnsValues011 = {"e0","j5","b1"};
		DBApp.select("vo", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"d","j"};
		String[] ConditionColumnsValues012 = {"d1","j5"};
		DBApp.select("vo", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("vo");
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"e","a","g","h","k"};
		String[] ConditionColumnsValues015 = {"e1","a41","g6","h1","k8"};
		DBApp.select("vo", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"a","g"};
		String[] ConditionColumnsValues017 = {"a187","g5"};
		DBApp.select("vo", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"d"};
		String[] ConditionColumnsValues019 = {"d3"};
		DBApp.select("vo", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"d"};
		String[] ConditionColumnsValues020 = {"d0"};
		DBApp.select("vo", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("vo");
	
		DBApp.select("vo");
	
		DBApp.select("vo");
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((251%DBApp.dataPageSize))-1;
		DBApp.select("vo", pageSelectPointer024,recordSelectPointer024);
	
		String[] ConditionColumns025 = {"b"};
		String[] ConditionColumnsValues025 = {"b1"};
		DBApp.select("vo", ConditionColumns025, ConditionColumnsValues025);
	
		DBApp.select("vo");
	
		String selectTrace0 = DBApp.getFullTrace("vo");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 280);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 251"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("em03", cols1);
		String [][] records_em03 = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			records_em03[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_em03[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("em03", records_em03[i]);
		}
	
		int pageCount1 = (int)Math.ceil(38.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns10 = {"h"};
		String[] ConditionColumnsValues10 = {"h2"};
		DBApp.select("em03", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("em03");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"g"};
		String[] ConditionColumnsValues13 = {"g2"};
		DBApp.select("em03", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"i"};
		String[] ConditionColumnsValues14 = {"i3"};
		DBApp.select("em03", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("em03");
	
		DBApp.select("em03");
	
		String[] ConditionColumns17 = {"c","h"};
		String[] ConditionColumnsValues17 = {"c1","h7"};
		DBApp.select("em03", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("em03");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b0"};
		DBApp.select("em03", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("em03");
	
		String[] ConditionColumns113 = {"h"};
		String[] ConditionColumnsValues113 = {"h2"};
		DBApp.select("em03", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("em03");
	
		String[] ConditionColumns115 = {"f"};
		String[] ConditionColumnsValues115 = {"f1"};
		DBApp.select("em03", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("em03");
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("em03");
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer121,recordSelectPointer121);
	
		String[] ConditionColumns122 = {"i"};
		String[] ConditionColumnsValues122 = {"i2"};
		DBApp.select("em03", ConditionColumns122, ConditionColumnsValues122);
	
		int pageSelectPointer123 = genRandNum(pageCount1)-1;
		int recordSelectPointer123 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer123==pageCount1-1)
			recordSelectPointer123 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer123,recordSelectPointer123);
	
		DBApp.select("em03");
	
		String[] ConditionColumns125 = {"d"};
		String[] ConditionColumnsValues125 = {"d1"};
		DBApp.select("em03", ConditionColumns125, ConditionColumnsValues125);
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("em03", pageSelectPointer126,recordSelectPointer126);
	
		DBApp.select("em03");
	
		String selectTrace1 = DBApp.getFullTrace("em03");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 68);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 38"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ljx5", cols2);
		String [][] records_ljx5 = new String[484][cols2.length];
		for(int i=0;i<484;i++)
		{
			records_ljx5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ljx5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ljx5", records_ljx5[i]);
		}
	
		int pageCount2 = (int)Math.ceil(484.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("ljx5");
	
		DBApp.select("ljx5");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"e","b"};
		String[] ConditionColumnsValues23 = {"e1","b1"};
		DBApp.select("ljx5", ConditionColumns23, ConditionColumnsValues23);
	
		DBApp.select("ljx5");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("ljx5");
	
		DBApp.select("ljx5");
	
		DBApp.select("ljx5");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"b","i","c"};
		String[] ConditionColumnsValues211 = {"b0","i4","c1"};
		DBApp.select("ljx5", ConditionColumns211, ConditionColumnsValues211);
	
		DBApp.select("ljx5");
	
		DBApp.select("ljx5");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"a"};
		String[] ConditionColumnsValues215 = {"a463"};
		DBApp.select("ljx5", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer216,recordSelectPointer216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer217,recordSelectPointer217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer218,recordSelectPointer218);
	
		DBApp.select("ljx5");
	
		DBApp.select("ljx5");
	
		String[] ConditionColumns221 = {"b"};
		String[] ConditionColumnsValues221 = {"b0"};
		DBApp.select("ljx5", ConditionColumns221, ConditionColumnsValues221);
	
		DBApp.select("ljx5");
	
		String[] ConditionColumns223 = {"a"};
		String[] ConditionColumnsValues223 = {"a46"};
		DBApp.select("ljx5", ConditionColumns223, ConditionColumnsValues223);
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer224,recordSelectPointer224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer225,recordSelectPointer225);
	
		String[] ConditionColumns226 = {"c"};
		String[] ConditionColumnsValues226 = {"c0"};
		DBApp.select("ljx5", ConditionColumns226, ConditionColumnsValues226);
	
		int pageSelectPointer227 = genRandNum(pageCount2)-1;
		int recordSelectPointer227 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer227==pageCount2-1)
			recordSelectPointer227 = genRandNum((484%DBApp.dataPageSize))-1;
		DBApp.select("ljx5", pageSelectPointer227,recordSelectPointer227);
	
		DBApp.select("ljx5");
	
		String selectTrace2 = DBApp.getFullTrace("ljx5");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 515);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 484"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("pc", cols0);
		String [][] records_pc = new String[143][cols0.length];
		for(int i=0;i<143;i++)
		{
			records_pc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_pc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("pc", records_pc[i]);
		}
	
		int pageCount0 = (int)Math.ceil(143.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a100"};
		DBApp.select("pc", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a109"};
		DBApp.select("pc", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a134"};
		DBApp.select("pc", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("pc");
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a39"};
		DBApp.select("pc", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("pc");
	
		DBApp.select("pc");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("pc");
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a101"};
		DBApp.select("pc", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("pc");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("pc");
	
		DBApp.select("pc");
	
		String[] ConditionColumns016 = {"a"};
		String[] ConditionColumnsValues016 = {"a17"};
		DBApp.select("pc", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"a"};
		String[] ConditionColumnsValues017 = {"a64"};
		DBApp.select("pc", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("pc");
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer019,recordSelectPointer019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer020,recordSelectPointer020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((143%DBApp.dataPageSize))-1;
		DBApp.select("pc", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("pc");
	
		DBApp.select("pc");
	
		DBApp.select("pc");
	
		DBApp.select("pc");
	
		String[] ConditionColumns026 = {"a"};
		String[] ConditionColumnsValues026 = {"a131"};
		DBApp.select("pc", ConditionColumns026, ConditionColumnsValues026);
	
		String[] ConditionColumns027 = {"a"};
		String[] ConditionColumnsValues027 = {"a48"};
		DBApp.select("pc", ConditionColumns027, ConditionColumnsValues027);
	
		DBApp.select("pc");
	
		String selectTrace0 = DBApp.getFullTrace("pc");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 174);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 143"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("f1m", cols1);
		String [][] records_f1m = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			records_f1m[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f1m[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f1m", records_f1m[i]);
		}
	
		int pageCount1 = (int)Math.ceil(13.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		String[] ConditionColumns10 = {"a"};
		String[] ConditionColumnsValues10 = {"a8"};
		DBApp.select("f1m", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"a"};
		String[] ConditionColumnsValues12 = {"a8"};
		DBApp.select("f1m", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("f1m");
	
		DBApp.select("f1m");
	
		DBApp.select("f1m");
	
		DBApp.select("f1m");
	
		DBApp.select("f1m");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((13%DBApp.dataPageSize))-1;
		DBApp.select("f1m", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("f1m");
	
		DBApp.select("f1m");
	
		DBApp.select("f1m");
	
		String selectTrace1 = DBApp.getFullTrace("f1m");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 33);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 13"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b5", cols2);
		String [][] records_b5 = new String[459][cols2.length];
		for(int i=0;i<459;i++)
		{
			records_b5[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b5[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b5", records_b5[i]);
		}
	
		int pageCount2 = (int)Math.ceil(459.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		String[] ConditionColumns20 = {"b","e","d","k","o"};
		String[] ConditionColumnsValues20 = {"b0","e1","d2","k10","o1"};
		DBApp.select("b5", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("b5");
	
		String[] ConditionColumns22 = {"i"};
		String[] ConditionColumnsValues22 = {"i8"};
		DBApp.select("b5", ConditionColumns22, ConditionColumnsValues22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((459%DBApp.dataPageSize))-1;
		DBApp.select("b5", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("b5");
	
		DBApp.select("b5");
	
		DBApp.select("b5");
	
		String[] ConditionColumns27 = {"o"};
		String[] ConditionColumnsValues27 = {"o12"};
		DBApp.select("b5", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"j","o","n","g","f","b","i"};
		String[] ConditionColumnsValues28 = {"j6","o11","n4","g4","f2","b0","i2"};
		DBApp.select("b5", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("b5");
	
		String[] ConditionColumns210 = {"h"};
		String[] ConditionColumnsValues210 = {"h5"};
		DBApp.select("b5", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((459%DBApp.dataPageSize))-1;
		DBApp.select("b5", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("b5");
	
		String selectTrace2 = DBApp.getFullTrace("b5");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 474);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 459"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("pp", cols3);
		String [][] records_pp = new String[71][cols3.length];
		for(int i=0;i<71;i++)
		{
			records_pp[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_pp[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("pp", records_pp[i]);
		}
	
		int pageCount3 = (int)Math.ceil(71.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		DBApp.select("pp");
	
		String[] ConditionColumns31 = {"f","b","g","c"};
		String[] ConditionColumnsValues31 = {"f1","b1","g0","c1"};
		DBApp.select("pp", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"f","a","e"};
		String[] ConditionColumnsValues32 = {"f2","a14","e4"};
		DBApp.select("pp", ConditionColumns32, ConditionColumnsValues32);
	
		String[] ConditionColumns33 = {"b"};
		String[] ConditionColumnsValues33 = {"b0"};
		DBApp.select("pp", ConditionColumns33, ConditionColumnsValues33);
	
		String[] ConditionColumns34 = {"a"};
		String[] ConditionColumnsValues34 = {"a39"};
		DBApp.select("pp", ConditionColumns34, ConditionColumnsValues34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer35,recordSelectPointer35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer36,recordSelectPointer36);
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer37,recordSelectPointer37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer38,recordSelectPointer38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("pp");
	
		DBApp.select("pp");
	
		String[] ConditionColumns312 = {"g"};
		String[] ConditionColumnsValues312 = {"g5"};
		DBApp.select("pp", ConditionColumns312, ConditionColumnsValues312);
	
		DBApp.select("pp");
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer314,recordSelectPointer314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((71%DBApp.dataPageSize))-1;
		DBApp.select("pp", pageSelectPointer315,recordSelectPointer315);
	
		String[] ConditionColumns316 = {"c"};
		String[] ConditionColumnsValues316 = {"c2"};
		DBApp.select("pp", ConditionColumns316, ConditionColumnsValues316);
	
		String[] ConditionColumns317 = {"h"};
		String[] ConditionColumnsValues317 = {"h2"};
		DBApp.select("pp", ConditionColumns317, ConditionColumnsValues317);
	
		String selectTrace3 = DBApp.getFullTrace("pp");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 91);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 71"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("sw", cols4);
		String [][] records_sw = new String[249][cols4.length];
		for(int i=0;i<249;i++)
		{
			records_sw[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_sw[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("sw", records_sw[i]);
		}
	
		int pageCount4 = (int)Math.ceil(249.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer40,recordSelectPointer40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer41,recordSelectPointer41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"i","b"};
		String[] ConditionColumnsValues43 = {"i4","b0"};
		DBApp.select("sw", ConditionColumns43, ConditionColumnsValues43);
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer44,recordSelectPointer44);
	
		String[] ConditionColumns45 = {"f"};
		String[] ConditionColumnsValues45 = {"f4"};
		DBApp.select("sw", ConditionColumns45, ConditionColumnsValues45);
	
		String[] ConditionColumns46 = {"c","d","e","b","j"};
		String[] ConditionColumnsValues46 = {"c0","d2","e0","b0","j0"};
		DBApp.select("sw", ConditionColumns46, ConditionColumnsValues46);
	
		DBApp.select("sw");
	
		int pageSelectPointer48 = genRandNum(pageCount4)-1;
		int recordSelectPointer48 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer48==pageCount4-1)
			recordSelectPointer48 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer48,recordSelectPointer48);
	
		String[] ConditionColumns49 = {"k"};
		String[] ConditionColumnsValues49 = {"k3"};
		DBApp.select("sw", ConditionColumns49, ConditionColumnsValues49);
	
		String[] ConditionColumns410 = {"f"};
		String[] ConditionColumnsValues410 = {"f1"};
		DBApp.select("sw", ConditionColumns410, ConditionColumnsValues410);
	
		DBApp.select("sw");
	
		int pageSelectPointer412 = genRandNum(pageCount4)-1;
		int recordSelectPointer412 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer412==pageCount4-1)
			recordSelectPointer412 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer412,recordSelectPointer412);
	
		String[] ConditionColumns413 = {"f","d"};
		String[] ConditionColumnsValues413 = {"f0","d0"};
		DBApp.select("sw", ConditionColumns413, ConditionColumnsValues413);
	
		String[] ConditionColumns414 = {"f"};
		String[] ConditionColumnsValues414 = {"f5"};
		DBApp.select("sw", ConditionColumns414, ConditionColumnsValues414);
	
		int pageSelectPointer415 = genRandNum(pageCount4)-1;
		int recordSelectPointer415 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer415==pageCount4-1)
			recordSelectPointer415 = genRandNum((249%DBApp.dataPageSize))-1;
		DBApp.select("sw", pageSelectPointer415,recordSelectPointer415);
	
		String[] ConditionColumns416 = {"b"};
		String[] ConditionColumnsValues416 = {"b0"};
		DBApp.select("sw", ConditionColumns416, ConditionColumnsValues416);
	
		DBApp.select("sw");
	
		DBApp.select("sw");
	
		String[] ConditionColumns419 = {"k","d","i"};
		String[] ConditionColumnsValues419 = {"k5","d3","i7"};
		DBApp.select("sw", ConditionColumns419, ConditionColumnsValues419);
	
		DBApp.select("sw");
	
		DBApp.select("sw");
	
		DBApp.select("sw");
	
		DBApp.select("sw");
	
		DBApp.select("sw");
	
		String[] ConditionColumns425 = {"j","c","f","e"};
		String[] ConditionColumnsValues425 = {"j0","c2","f2","e0"};
		DBApp.select("sw", ConditionColumns425, ConditionColumnsValues425);
	
		String selectTrace4 = DBApp.getFullTrace("sw");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 277);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 249"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("zx7", cols0);
		String [][] records_zx7 = new String[258][cols0.length];
		for(int i=0;i<258;i++)
		{
			records_zx7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zx7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zx7", records_zx7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(258.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"b","e"};
		String[] ConditionColumnsValues04 = {"b0","e3"};
		DBApp.select("zx7", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"k"};
		String[] ConditionColumnsValues05 = {"k8"};
		DBApp.select("zx7", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("zx7");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"i"};
		String[] ConditionColumnsValues011 = {"i7"};
		DBApp.select("zx7", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("zx7");
	
		DBApp.select("zx7");
	
		String[] ConditionColumns014 = {"i","k","d","h","b"};
		String[] ConditionColumnsValues014 = {"i4","k7","d2","h6","b0"};
		DBApp.select("zx7", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("zx7");
	
		String[] ConditionColumns018 = {"g","b","j","e","k"};
		String[] ConditionColumnsValues018 = {"g1","b1","j3","e3","k10"};
		DBApp.select("zx7", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"i","f","h","j","b"};
		String[] ConditionColumnsValues019 = {"i1","f1","h7","j9","b1"};
		DBApp.select("zx7", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"e"};
		String[] ConditionColumnsValues020 = {"e1"};
		DBApp.select("zx7", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((258%DBApp.dataPageSize))-1;
		DBApp.select("zx7", pageSelectPointer021,recordSelectPointer021);
	
		String selectTrace0 = DBApp.getFullTrace("zx7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 282);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 258"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("jnfd8", cols0);
		String [][] records_jnfd8 = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			records_jnfd8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jnfd8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jnfd8", records_jnfd8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(96.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		String[] ConditionColumns00 = {"d"};
		String[] ConditionColumnsValues00 = {"d1"};
		DBApp.select("jnfd8", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b0"};
		DBApp.select("jnfd8", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("jnfd8", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("jnfd8");
	
		DBApp.select("jnfd8");
	
		DBApp.select("jnfd8");
	
		String[] ConditionColumns06 = {"c"};
		String[] ConditionColumnsValues06 = {"c2"};
		DBApp.select("jnfd8", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"b"};
		String[] ConditionColumnsValues07 = {"b1"};
		DBApp.select("jnfd8", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("jnfd8");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("jnfd8", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("jnfd8");
	
		String selectTrace0 = DBApp.getFullTrace("jnfd8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 109);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 96"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("qaa11", cols1);
		String [][] records_qaa11 = new String[326][cols1.length];
		for(int i=0;i<326;i++)
		{
			records_qaa11[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_qaa11[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("qaa11", records_qaa11[i]);
		}
	
		int pageCount1 = (int)Math.ceil(326.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("qaa11", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("qaa11", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("qaa11");
	
		String[] ConditionColumns13 = {"b"};
		String[] ConditionColumnsValues13 = {"b0"};
		DBApp.select("qaa11", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("qaa11", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"a"};
		String[] ConditionColumnsValues15 = {"a50"};
		DBApp.select("qaa11", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b0"};
		DBApp.select("qaa11", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b0"};
		DBApp.select("qaa11", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("qaa11");
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b0"};
		DBApp.select("qaa11", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("qaa11", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("qaa11");
	
		DBApp.select("qaa11");
	
		DBApp.select("qaa11");
	
		String[] ConditionColumns114 = {"b"};
		String[] ConditionColumnsValues114 = {"b1"};
		DBApp.select("qaa11", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("qaa11", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("qaa11", pageSelectPointer116,recordSelectPointer116);
	
		String selectTrace1 = DBApp.getFullTrace("qaa11");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 345);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 326"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("c8o3", cols0);
		String [][] records_c8o3 = new String[491][cols0.length];
		for(int i=0;i<491;i++)
		{
			records_c8o3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c8o3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c8o3", records_c8o3[i]);
		}
	
		int pageCount0 = (int)Math.ceil(491.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns00 = {"e","g","b"};
		String[] ConditionColumnsValues00 = {"e3","g3","b1"};
		DBApp.select("c8o3", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("c8o3");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("c8o3", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"b","d","a"};
		String[] ConditionColumnsValues03 = {"b1","d1","a353"};
		DBApp.select("c8o3", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("c8o3", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("c8o3", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("c8o3", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"h"};
		String[] ConditionColumnsValues07 = {"h6"};
		DBApp.select("c8o3", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"a","c","h"};
		String[] ConditionColumnsValues08 = {"a447","c0","h7"};
		DBApp.select("c8o3", ConditionColumns08, ConditionColumnsValues08);
	
		String selectTrace0 = DBApp.getFullTrace("c8o3");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 502);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 491"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("u95", cols0);
		String [][] records_u95 = new String[326][cols0.length];
		for(int i=0;i<326;i++)
		{
			records_u95[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_u95[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("u95", records_u95[i]);
		}
	
		int pageCount0 = (int)Math.ceil(326.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a123"};
		DBApp.select("u95", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("u95", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("u95", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a243"};
		DBApp.select("u95", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("u95");
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a58"};
		DBApp.select("u95", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("u95");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("u95", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("u95");
	
		DBApp.select("u95");
	
		String[] ConditionColumns010 = {"a"};
		String[] ConditionColumnsValues010 = {"a197"};
		DBApp.select("u95", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("u95");
	
		String[] ConditionColumns012 = {"a"};
		String[] ConditionColumnsValues012 = {"a282"};
		DBApp.select("u95", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("u95", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("u95");
	
		DBApp.select("u95");
	
		String[] ConditionColumns016 = {"a"};
		String[] ConditionColumnsValues016 = {"a176"};
		DBApp.select("u95", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((326%DBApp.dataPageSize))-1;
		DBApp.select("u95", pageSelectPointer017,recordSelectPointer017);
	
		DBApp.select("u95");
	
		DBApp.select("u95");
	
		String[] ConditionColumns020 = {"a"};
		String[] ConditionColumnsValues020 = {"a5"};
		DBApp.select("u95", ConditionColumns020, ConditionColumnsValues020);
	
		String[] ConditionColumns021 = {"a"};
		String[] ConditionColumnsValues021 = {"a103"};
		DBApp.select("u95", ConditionColumns021, ConditionColumnsValues021);
	
		DBApp.select("u95");
	
		String selectTrace0 = DBApp.getFullTrace("u95");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 351);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 326"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("lbx", cols1);
		String [][] records_lbx = new String[361][cols1.length];
		for(int i=0;i<361;i++)
		{
			records_lbx[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_lbx[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("lbx", records_lbx[i]);
		}
	
		int pageCount1 = (int)Math.ceil(361.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		DBApp.select("lbx");
	
		String selectTrace1 = DBApp.getFullTrace("lbx");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 364);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 361"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("j95m9", cols2);
		String [][] records_j95m9 = new String[323][cols2.length];
		for(int i=0;i<323;i++)
		{
			records_j95m9[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j95m9[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j95m9", records_j95m9[i]);
		}
	
		int pageCount2 = (int)Math.ceil(323.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		String[] ConditionColumns20 = {"c"};
		String[] ConditionColumnsValues20 = {"c0"};
		DBApp.select("j95m9", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("j95m9");
	
		String selectTrace2 = DBApp.getFullTrace("j95m9");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 327);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 323"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("u62e", cols3);
		String [][] records_u62e = new String[224][cols3.length];
		for(int i=0;i<224;i++)
		{
			records_u62e[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u62e[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u62e", records_u62e[i]);
		}
	
		int pageCount3 = (int)Math.ceil(224.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("u62e", pageSelectPointer30,recordSelectPointer30);
	
		DBApp.select("u62e");
	
		String[] ConditionColumns32 = {"a"};
		String[] ConditionColumnsValues32 = {"a64"};
		DBApp.select("u62e", ConditionColumns32, ConditionColumnsValues32);
	
		String selectTrace3 = DBApp.getFullTrace("u62e");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 229);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 224"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("m6i", cols0);
		String [][] records_m6i = new String[430][cols0.length];
		for(int i=0;i<430;i++)
		{
			records_m6i[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m6i[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m6i", records_m6i[i]);
		}
	
		int pageCount0 = (int)Math.ceil(430.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		String[] ConditionColumns00 = {"k","f"};
		String[] ConditionColumnsValues00 = {"k6","f0"};
		DBApp.select("m6i", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("m6i", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("m6i");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("m6i", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"c","f","g","l","h"};
		String[] ConditionColumnsValues04 = {"c1","f1","g0","l1","h1"};
		DBApp.select("m6i", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("m6i");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((430%DBApp.dataPageSize))-1;
		DBApp.select("m6i", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"b","j"};
		String[] ConditionColumnsValues07 = {"b0","j8"};
		DBApp.select("m6i", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("m6i");
	
		DBApp.select("m6i");
	
		String selectTrace0 = DBApp.getFullTrace("m6i");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 442);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 430"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("ua", cols0);
		String [][] records_ua = new String[152][cols0.length];
		for(int i=0;i<152;i++)
		{
			records_ua[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ua[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ua", records_ua[i]);
		}
	
		int pageCount0 = (int)Math.ceil(152.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		DBApp.select("ua");
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a150"};
		DBApp.select("ua", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("ua");
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a93"};
		DBApp.select("ua", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("ua");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"a"};
		String[] ConditionColumnsValues010 = {"a140"};
		DBApp.select("ua", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"a"};
		String[] ConditionColumnsValues011 = {"a12"};
		DBApp.select("ua", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("ua");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"a"};
		String[] ConditionColumnsValues016 = {"a122"};
		DBApp.select("ua", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((152%DBApp.dataPageSize))-1;
		DBApp.select("ua", pageSelectPointer018,recordSelectPointer018);
	
		String selectTrace0 = DBApp.getFullTrace("ua");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 173);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 152"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("gnp7j", cols1);
		String [][] records_gnp7j = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			records_gnp7j[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gnp7j[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gnp7j", records_gnp7j[i]);
		}
	
		int pageCount1 = (int)Math.ceil(31.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("gnp7j");
	
		DBApp.select("gnp7j");
	
		String[] ConditionColumns12 = {"e","b","g"};
		String[] ConditionColumnsValues12 = {"e2","b0","g2"};
		DBApp.select("gnp7j", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((31%DBApp.dataPageSize))-1;
		DBApp.select("gnp7j", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("gnp7j");
	
		DBApp.select("gnp7j");
	
		String[] ConditionColumns16 = {"c"};
		String[] ConditionColumnsValues16 = {"c1"};
		DBApp.select("gnp7j", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("gnp7j");
	
		DBApp.select("gnp7j");
	
		String[] ConditionColumns19 = {"c","b"};
		String[] ConditionColumnsValues19 = {"c1","b0"};
		DBApp.select("gnp7j", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("gnp7j");
	
		String[] ConditionColumns111 = {"i","f","h"};
		String[] ConditionColumnsValues111 = {"i5","f5","h7"};
		DBApp.select("gnp7j", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("gnp7j");
	
		DBApp.select("gnp7j");
	
		DBApp.select("gnp7j");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((31%DBApp.dataPageSize))-1;
		DBApp.select("gnp7j", pageSelectPointer115,recordSelectPointer115);
	
		String selectTrace1 = DBApp.getFullTrace("gnp7j");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 49);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 31"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("czh", cols2);
		String [][] records_czh = new String[311][cols2.length];
		for(int i=0;i<311;i++)
		{
			records_czh[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_czh[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("czh", records_czh[i]);
		}
	
		int pageCount2 = (int)Math.ceil(311.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"c","b","f","i"};
		String[] ConditionColumnsValues23 = {"c0","b0","f0","i6"};
		DBApp.select("czh", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"g","f"};
		String[] ConditionColumnsValues24 = {"g6","f1"};
		DBApp.select("czh", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("czh");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("czh");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer29,recordSelectPointer29);
	
		String[] ConditionColumns210 = {"f","h","j","g"};
		String[] ConditionColumnsValues210 = {"f3","h7","j9","g5"};
		DBApp.select("czh", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((311%DBApp.dataPageSize))-1;
		DBApp.select("czh", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("czh");
	
		String selectTrace2 = DBApp.getFullTrace("czh");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 326);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 311"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("al", cols0);
		String [][] records_al = new String[191][cols0.length];
		for(int i=0;i<191;i++)
		{
			records_al[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_al[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("al", records_al[i]);
		}
	
		int pageCount0 = (int)Math.ceil(191.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns00 = {"d"};
		String[] ConditionColumnsValues00 = {"d3"};
		DBApp.select("al", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b1"};
		DBApp.select("al", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"c"};
		String[] ConditionColumnsValues02 = {"c1"};
		DBApp.select("al", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b0"};
		DBApp.select("al", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("al");
	
		DBApp.select("al");
	
		String[] ConditionColumns07 = {"a"};
		String[] ConditionColumnsValues07 = {"a70"};
		DBApp.select("al", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"e"};
		String[] ConditionColumnsValues09 = {"e2"};
		DBApp.select("al", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("al");
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer014,recordSelectPointer014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer016,recordSelectPointer016);
	
		DBApp.select("al");
	
		DBApp.select("al");
	
		DBApp.select("al");
	
		String[] ConditionColumns020 = {"c"};
		String[] ConditionColumnsValues020 = {"c1"};
		DBApp.select("al", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer021,recordSelectPointer021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((191%DBApp.dataPageSize))-1;
		DBApp.select("al", pageSelectPointer022,recordSelectPointer022);
	
		DBApp.select("al");
	
		DBApp.select("al");
	
		String selectTrace0 = DBApp.getFullTrace("al");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 218);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 191"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("lz72o", cols0);
		String [][] records_lz72o = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			records_lz72o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lz72o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lz72o", records_lz72o[i]);
		}
	
		int pageCount0 = (int)Math.ceil(65.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a38"};
		DBApp.select("lz72o", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a22"};
		DBApp.select("lz72o", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("lz72o");
	
		DBApp.select("lz72o");
	
		DBApp.select("lz72o");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((65%DBApp.dataPageSize))-1;
		DBApp.select("lz72o", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("lz72o");
	
		String[] ConditionColumns07 = {"a"};
		String[] ConditionColumnsValues07 = {"a19"};
		DBApp.select("lz72o", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"a"};
		String[] ConditionColumnsValues08 = {"a48"};
		DBApp.select("lz72o", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a36"};
		DBApp.select("lz72o", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((65%DBApp.dataPageSize))-1;
		DBApp.select("lz72o", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"a"};
		String[] ConditionColumnsValues011 = {"a14"};
		DBApp.select("lz72o", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"a"};
		String[] ConditionColumnsValues012 = {"a43"};
		DBApp.select("lz72o", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("lz72o");
	
		String[] ConditionColumns014 = {"a"};
		String[] ConditionColumnsValues014 = {"a13"};
		DBApp.select("lz72o", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"a"};
		String[] ConditionColumnsValues015 = {"a28"};
		DBApp.select("lz72o", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"a"};
		String[] ConditionColumnsValues016 = {"a49"};
		DBApp.select("lz72o", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"a"};
		String[] ConditionColumnsValues017 = {"a15"};
		DBApp.select("lz72o", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"a"};
		String[] ConditionColumnsValues018 = {"a59"};
		DBApp.select("lz72o", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"a"};
		String[] ConditionColumnsValues019 = {"a61"};
		DBApp.select("lz72o", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("lz72o");
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((65%DBApp.dataPageSize))-1;
		DBApp.select("lz72o", pageSelectPointer021,recordSelectPointer021);
	
		String selectTrace0 = DBApp.getFullTrace("lz72o");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 89);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 65"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k6", cols1);
		String [][] records_k6 = new String[254][cols1.length];
		for(int i=0;i<254;i++)
		{
			records_k6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_k6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("k6", records_k6[i]);
		}
	
		int pageCount1 = (int)Math.ceil(254.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		DBApp.select("k6");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("k6");
	
		String[] ConditionColumns14 = {"d"};
		String[] ConditionColumnsValues14 = {"d2"};
		DBApp.select("k6", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"b","d","c","f"};
		String[] ConditionColumnsValues16 = {"b0","d0","c1","f4"};
		DBApp.select("k6", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"i"};
		String[] ConditionColumnsValues18 = {"i7"};
		DBApp.select("k6", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("k6");
	
		DBApp.select("k6");
	
		DBApp.select("k6");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("k6");
	
		DBApp.select("k6");
	
		String[] ConditionColumns116 = {"e"};
		String[] ConditionColumnsValues116 = {"e3"};
		DBApp.select("k6", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"i","d","h","g","c"};
		String[] ConditionColumnsValues117 = {"i6","d0","h0","g5","c0"};
		DBApp.select("k6", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("k6");
	
		DBApp.select("k6");
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((254%DBApp.dataPageSize))-1;
		DBApp.select("k6", pageSelectPointer121,recordSelectPointer121);
	
		String selectTrace1 = DBApp.getFullTrace("k6");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 278);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 254"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("bc", cols2);
		String [][] records_bc = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			records_bc[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bc[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bc", records_bc[i]);
		}
	
		int pageCount2 = (int)Math.ceil(56.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns20 = {"c"};
		String[] ConditionColumnsValues20 = {"c0"};
		DBApp.select("bc", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"c"};
		String[] ConditionColumnsValues21 = {"c2"};
		DBApp.select("bc", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer26,recordSelectPointer26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"c"};
		String[] ConditionColumnsValues28 = {"c2"};
		DBApp.select("bc", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("bc");
	
		DBApp.select("bc");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("bc");
	
		DBApp.select("bc");
	
		DBApp.select("bc");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("bc");
	
		String[] ConditionColumns217 = {"b"};
		String[] ConditionColumnsValues217 = {"b1"};
		DBApp.select("bc", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("bc");
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((56%DBApp.dataPageSize))-1;
		DBApp.select("bc", pageSelectPointer219,recordSelectPointer219);
	
		String[] ConditionColumns220 = {"b"};
		String[] ConditionColumnsValues220 = {"b0"};
		DBApp.select("bc", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"c"};
		String[] ConditionColumnsValues221 = {"c0"};
		DBApp.select("bc", ConditionColumns221, ConditionColumnsValues221);
	
		DBApp.select("bc");
	
		String[] ConditionColumns223 = {"c"};
		String[] ConditionColumnsValues223 = {"c2"};
		DBApp.select("bc", ConditionColumns223, ConditionColumnsValues223);
	
		String[] ConditionColumns224 = {"c"};
		String[] ConditionColumnsValues224 = {"c2"};
		DBApp.select("bc", ConditionColumns224, ConditionColumnsValues224);
	
		String selectTrace2 = DBApp.getFullTrace("bc");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 83);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 56"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("jy7", cols0);
		String [][] records_jy7 = new String[358][cols0.length];
		for(int i=0;i<358;i++)
		{
			records_jy7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jy7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jy7", records_jy7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(358.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((358%DBApp.dataPageSize))-1;
		DBApp.select("jy7", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("jy7");
	
		String[] ConditionColumns02 = {"k","d","l"};
		String[] ConditionColumnsValues02 = {"k7","d3","l11"};
		DBApp.select("jy7", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("jy7");
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d0"};
		DBApp.select("jy7", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("jy7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 365);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 358"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("ut4", cols1);
		String [][] records_ut4 = new String[52][cols1.length];
		for(int i=0;i<52;i++)
		{
			records_ut4[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ut4[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ut4", records_ut4[i]);
		}
	
		int pageCount1 = (int)Math.ceil(52.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("ut4", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("ut4");
	
		String[] ConditionColumns12 = {"d"};
		String[] ConditionColumnsValues12 = {"d3"};
		DBApp.select("ut4", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("ut4");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("ut4", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"b"};
		String[] ConditionColumnsValues15 = {"b1"};
		DBApp.select("ut4", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"a"};
		String[] ConditionColumnsValues16 = {"a14"};
		DBApp.select("ut4", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"d"};
		String[] ConditionColumnsValues17 = {"d3"};
		DBApp.select("ut4", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("ut4");
	
		DBApp.select("ut4");
	
		DBApp.select("ut4");
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b1"};
		DBApp.select("ut4", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("ut4", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("ut4", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("ut4");
	
		String[] ConditionColumns115 = {"b"};
		String[] ConditionColumnsValues115 = {"b1"};
		DBApp.select("ut4", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"a"};
		String[] ConditionColumnsValues116 = {"a51"};
		DBApp.select("ut4", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"c"};
		String[] ConditionColumnsValues117 = {"c0"};
		DBApp.select("ut4", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"d"};
		String[] ConditionColumnsValues118 = {"d3"};
		DBApp.select("ut4", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("ut4");
	
		DBApp.select("ut4");
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b1"};
		DBApp.select("ut4", ConditionColumns121, ConditionColumnsValues121);
	
		String[] ConditionColumns122 = {"d"};
		String[] ConditionColumnsValues122 = {"d2"};
		DBApp.select("ut4", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"d","a"};
		String[] ConditionColumnsValues123 = {"d1","a41"};
		DBApp.select("ut4", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"b"};
		String[] ConditionColumnsValues124 = {"b0"};
		DBApp.select("ut4", ConditionColumns124, ConditionColumnsValues124);
	
		DBApp.select("ut4");
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("ut4", pageSelectPointer126,recordSelectPointer126);
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("ut4", pageSelectPointer127,recordSelectPointer127);
	
		DBApp.select("ut4");
	
		String[] ConditionColumns129 = {"a"};
		String[] ConditionColumnsValues129 = {"a47"};
		DBApp.select("ut4", ConditionColumns129, ConditionColumnsValues129);
	
		String selectTrace1 = DBApp.getFullTrace("ut4");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 84);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 52"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("tn6a", cols2);
		String [][] records_tn6a = new String[500][cols2.length];
		for(int i=0;i<500;i++)
		{
			records_tn6a[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_tn6a[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("tn6a", records_tn6a[i]);
		}
	
		int pageCount2 = (int)Math.ceil(500.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("tn6a");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("tn6a");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("tn6a");
	
		DBApp.select("tn6a");
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("tn6a");
	
		String[] ConditionColumns28 = {"a"};
		String[] ConditionColumnsValues28 = {"a65"};
		DBApp.select("tn6a", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"a"};
		String[] ConditionColumnsValues29 = {"a332"};
		DBApp.select("tn6a", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer210,recordSelectPointer210);
	
		DBApp.select("tn6a");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer212,recordSelectPointer212);
	
		String[] ConditionColumns213 = {"a"};
		String[] ConditionColumnsValues213 = {"a218"};
		DBApp.select("tn6a", ConditionColumns213, ConditionColumnsValues213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer214,recordSelectPointer214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"a"};
		String[] ConditionColumnsValues216 = {"a494"};
		DBApp.select("tn6a", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"a"};
		String[] ConditionColumnsValues217 = {"a140"};
		DBApp.select("tn6a", ConditionColumns217, ConditionColumnsValues217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer218,recordSelectPointer218);
	
		DBApp.select("tn6a");
	
		DBApp.select("tn6a");
	
		DBApp.select("tn6a");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer222,recordSelectPointer222);
	
		DBApp.select("tn6a");
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer224,recordSelectPointer224);
	
		String[] ConditionColumns225 = {"a"};
		String[] ConditionColumnsValues225 = {"a403"};
		DBApp.select("tn6a", ConditionColumns225, ConditionColumnsValues225);
	
		int pageSelectPointer226 = genRandNum(pageCount2)-1;
		int recordSelectPointer226 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer226==pageCount2-1)
			recordSelectPointer226 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer226,recordSelectPointer226);
	
		int pageSelectPointer227 = genRandNum(pageCount2)-1;
		int recordSelectPointer227 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer227==pageCount2-1)
			recordSelectPointer227 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer227,recordSelectPointer227);
	
		String[] ConditionColumns228 = {"a"};
		String[] ConditionColumnsValues228 = {"a199"};
		DBApp.select("tn6a", ConditionColumns228, ConditionColumnsValues228);
	
		int pageSelectPointer229 = genRandNum(pageCount2)-1;
		int recordSelectPointer229 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer229==pageCount2-1)
			recordSelectPointer229 = genRandNum((500%DBApp.dataPageSize))-1;
		DBApp.select("tn6a", pageSelectPointer229,recordSelectPointer229);
	
		String selectTrace2 = DBApp.getFullTrace("tn6a");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 532);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 500"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("wgpqq", cols3);
		String [][] records_wgpqq = new String[75][cols3.length];
		for(int i=0;i<75;i++)
		{
			records_wgpqq[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_wgpqq[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("wgpqq", records_wgpqq[i]);
		}
	
		int pageCount3 = (int)Math.ceil(75.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		String[] ConditionColumns30 = {"c","e"};
		String[] ConditionColumnsValues30 = {"c1","e0"};
		DBApp.select("wgpqq", ConditionColumns30, ConditionColumnsValues30);
	
		String[] ConditionColumns31 = {"h","f"};
		String[] ConditionColumnsValues31 = {"h5","f1"};
		DBApp.select("wgpqq", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"h"};
		String[] ConditionColumnsValues32 = {"h1"};
		DBApp.select("wgpqq", ConditionColumns32, ConditionColumnsValues32);
	
		String[] ConditionColumns33 = {"g"};
		String[] ConditionColumnsValues33 = {"g0"};
		DBApp.select("wgpqq", ConditionColumns33, ConditionColumnsValues33);
	
		DBApp.select("wgpqq");
	
		DBApp.select("wgpqq");
	
		String[] ConditionColumns36 = {"g"};
		String[] ConditionColumnsValues36 = {"g5"};
		DBApp.select("wgpqq", ConditionColumns36, ConditionColumnsValues36);
	
		String[] ConditionColumns37 = {"d"};
		String[] ConditionColumnsValues37 = {"d0"};
		DBApp.select("wgpqq", ConditionColumns37, ConditionColumnsValues37);
	
		DBApp.select("wgpqq");
	
		String[] ConditionColumns39 = {"g"};
		String[] ConditionColumnsValues39 = {"g0"};
		DBApp.select("wgpqq", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"a"};
		String[] ConditionColumnsValues310 = {"a55"};
		DBApp.select("wgpqq", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"f"};
		String[] ConditionColumnsValues311 = {"f5"};
		DBApp.select("wgpqq", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"d"};
		String[] ConditionColumnsValues312 = {"d1"};
		DBApp.select("wgpqq", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((75%DBApp.dataPageSize))-1;
		DBApp.select("wgpqq", pageSelectPointer313,recordSelectPointer313);
	
		DBApp.select("wgpqq");
	
		DBApp.select("wgpqq");
	
		DBApp.select("wgpqq");
	
		String[] ConditionColumns317 = {"g"};
		String[] ConditionColumnsValues317 = {"g2"};
		DBApp.select("wgpqq", ConditionColumns317, ConditionColumnsValues317);
	
		DBApp.select("wgpqq");
	
		DBApp.select("wgpqq");
	
		String selectTrace3 = DBApp.getFullTrace("wgpqq");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 97);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 75"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(774);
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