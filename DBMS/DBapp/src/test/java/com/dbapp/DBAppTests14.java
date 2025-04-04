package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests14
{

	@Test(timeout = 1000000)
	public void test19TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("d783t", cols0);
		String [][] records_d783t = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			records_d783t[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d783t[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d783t", records_d783t[i]);
		}
	
		int pageCount0 = (int)Math.ceil(70.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		DBApp.select("d783t");
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a5"};
		DBApp.select("d783t", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a34"};
		DBApp.select("d783t", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("d783t");
	
		DBApp.select("d783t");
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a19"};
		DBApp.select("d783t", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("d783t", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("d783t", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("d783t", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a55"};
		DBApp.select("d783t", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("d783t", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("d783t", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"a"};
		String[] ConditionColumnsValues012 = {"a11"};
		DBApp.select("d783t", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("d783t");
	
		String selectTrace0 = DBApp.getFullTrace("d783t");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 86);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 70"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("st96", cols1);
		String [][] records_st96 = new String[397][cols1.length];
		for(int i=0;i<397;i++)
		{
			records_st96[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_st96[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("st96", records_st96[i]);
		}
	
		int pageCount1 = (int)Math.ceil(397.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		DBApp.select("st96");
	
		DBApp.select("st96");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"c"};
		String[] ConditionColumnsValues16 = {"c1"};
		DBApp.select("st96", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("st96");
	
		String[] ConditionColumns18 = {"b"};
		String[] ConditionColumnsValues18 = {"b1"};
		DBApp.select("st96", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b1"};
		DBApp.select("st96", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"c"};
		String[] ConditionColumnsValues110 = {"c1"};
		DBApp.select("st96", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"c"};
		String[] ConditionColumnsValues111 = {"c0"};
		DBApp.select("st96", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"c"};
		String[] ConditionColumnsValues112 = {"c1"};
		DBApp.select("st96", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("st96");
	
		String[] ConditionColumns114 = {"b"};
		String[] ConditionColumnsValues114 = {"b0"};
		DBApp.select("st96", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"c"};
		String[] ConditionColumnsValues115 = {"c0"};
		DBApp.select("st96", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer116,recordSelectPointer116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer117,recordSelectPointer117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer118,recordSelectPointer118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((397%DBApp.dataPageSize))-1;
		DBApp.select("st96", pageSelectPointer119,recordSelectPointer119);
	
		DBApp.select("st96");
	
		String selectTrace1 = DBApp.getFullTrace("st96");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 420);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 397"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ao4", cols2);
		String [][] records_ao4 = new String[197][cols2.length];
		for(int i=0;i<197;i++)
		{
			records_ao4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ao4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ao4", records_ao4[i]);
		}
	
		int pageCount2 = (int)Math.ceil(197.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns20 = {"g","f"};
		String[] ConditionColumnsValues20 = {"g5","f2"};
		DBApp.select("ao4", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"b"};
		String[] ConditionColumnsValues21 = {"b1"};
		DBApp.select("ao4", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((197%DBApp.dataPageSize))-1;
		DBApp.select("ao4", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"c","b"};
		String[] ConditionColumnsValues23 = {"c0","b1"};
		DBApp.select("ao4", ConditionColumns23, ConditionColumnsValues23);
	
		String selectTrace2 = DBApp.getFullTrace("ao4");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 203);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 197"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("p152m", cols0);
		String [][] records_p152m = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			records_p152m[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p152m[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p152m", records_p152m[i]);
		}
	
		int pageCount0 = (int)Math.ceil(73.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		DBApp.select("p152m");
	
		DBApp.select("p152m");
	
		String[] ConditionColumns02 = {"l"};
		String[] ConditionColumnsValues02 = {"l9"};
		DBApp.select("p152m", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("p152m", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("p152m");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("p152m", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"i","j","o","k"};
		String[] ConditionColumnsValues06 = {"i8","j8","o8","k8"};
		DBApp.select("p152m", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("p152m");
	
		String[] ConditionColumns08 = {"b","i","m","k","o","f"};
		String[] ConditionColumnsValues08 = {"b1","i3","m0","k6","o9","f3"};
		DBApp.select("p152m", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"o"};
		String[] ConditionColumnsValues09 = {"o11"};
		DBApp.select("p152m", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("p152m");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("p152m", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("p152m");
	
		DBApp.select("p152m");
	
		String[] ConditionColumns014 = {"g","f","m"};
		String[] ConditionColumnsValues014 = {"g3","f2","m12"};
		DBApp.select("p152m", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"l"};
		String[] ConditionColumnsValues015 = {"l8"};
		DBApp.select("p152m", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"k","n","d"};
		String[] ConditionColumnsValues016 = {"k3","n8","d0"};
		DBApp.select("p152m", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"b"};
		String[] ConditionColumnsValues017 = {"b0"};
		DBApp.select("p152m", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("p152m");
	
		String[] ConditionColumns019 = {"n"};
		String[] ConditionColumnsValues019 = {"n3"};
		DBApp.select("p152m", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"m","e","j","k","h","l","f"};
		String[] ConditionColumnsValues020 = {"m8","e1","j1","k10","h5","l9","f3"};
		DBApp.select("p152m", ConditionColumns020, ConditionColumnsValues020);
	
		String[] ConditionColumns021 = {"g","o","j","k","f","n","m"};
		String[] ConditionColumnsValues021 = {"g4","o11","j1","k0","f5","n11","m11"};
		DBApp.select("p152m", ConditionColumns021, ConditionColumnsValues021);
	
		String selectTrace0 = DBApp.getFullTrace("p152m");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 97);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 73"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("w56z", cols1);
		String [][] records_w56z = new String[224][cols1.length];
		for(int i=0;i<224;i++)
		{
			records_w56z[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w56z[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w56z", records_w56z[i]);
		}
	
		int pageCount1 = (int)Math.ceil(224.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("w56z");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"d"};
		String[] ConditionColumnsValues12 = {"d1"};
		DBApp.select("w56z", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("w56z");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"c"};
		String[] ConditionColumnsValues16 = {"c0"};
		DBApp.select("w56z", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("w56z");
	
		DBApp.select("w56z");
	
		DBApp.select("w56z");
	
		DBApp.select("w56z");
	
		DBApp.select("w56z");
	
		String[] ConditionColumns112 = {"b"};
		String[] ConditionColumnsValues112 = {"b1"};
		DBApp.select("w56z", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("w56z");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("w56z");
	
		DBApp.select("w56z");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((224%DBApp.dataPageSize))-1;
		DBApp.select("w56z", pageSelectPointer119,recordSelectPointer119);
	
		String[] ConditionColumns120 = {"c"};
		String[] ConditionColumnsValues120 = {"c1"};
		DBApp.select("w56z", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"c"};
		String[] ConditionColumnsValues121 = {"c2"};
		DBApp.select("w56z", ConditionColumns121, ConditionColumnsValues121);
	
		String[] ConditionColumns122 = {"c"};
		String[] ConditionColumnsValues122 = {"c2"};
		DBApp.select("w56z", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"b","c"};
		String[] ConditionColumnsValues123 = {"b0","c0"};
		DBApp.select("w56z", ConditionColumns123, ConditionColumnsValues123);
	
		DBApp.select("w56z");
	
		String selectTrace1 = DBApp.getFullTrace("w56z");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 251);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 224"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("h6", cols0);
		String [][] records_h6 = new String[413][cols0.length];
		for(int i=0;i<413;i++)
		{
			records_h6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h6", records_h6[i]);
		}
	
		int pageCount0 = (int)Math.ceil(413.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((413%DBApp.dataPageSize))-1;
		DBApp.select("h6", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"g"};
		String[] ConditionColumnsValues01 = {"g5"};
		DBApp.select("h6", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("h6");
	
		String[] ConditionColumns03 = {"e","f","g","i","d"};
		String[] ConditionColumnsValues03 = {"e0","f1","g5","i4","d3"};
		DBApp.select("h6", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((413%DBApp.dataPageSize))-1;
		DBApp.select("h6", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("h6");
	
		DBApp.select("h6");
	
		String[] ConditionColumns07 = {"f","g","b","d"};
		String[] ConditionColumnsValues07 = {"f2","g5","b0","d2"};
		DBApp.select("h6", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("h6");
	
		String[] ConditionColumns09 = {"i"};
		String[] ConditionColumnsValues09 = {"i3"};
		DBApp.select("h6", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((413%DBApp.dataPageSize))-1;
		DBApp.select("h6", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("h6");
	
		DBApp.select("h6");
	
		DBApp.select("h6");
	
		DBApp.select("h6");
	
		DBApp.select("h6");
	
		String[] ConditionColumns016 = {"g"};
		String[] ConditionColumnsValues016 = {"g2"};
		DBApp.select("h6", ConditionColumns016, ConditionColumnsValues016);
	
		String selectTrace0 = DBApp.getFullTrace("h6");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 432);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 413"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s2s", cols0);
		String [][] records_s2s = new String[446][cols0.length];
		for(int i=0;i<446;i++)
		{
			records_s2s[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s2s[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s2s", records_s2s[i]);
		}
	
		int pageCount0 = (int)Math.ceil(446.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		String[] ConditionColumns00 = {"d"};
		String[] ConditionColumnsValues00 = {"d2"};
		DBApp.select("s2s", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((446%DBApp.dataPageSize))-1;
		DBApp.select("s2s", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"d","g"};
		String[] ConditionColumnsValues02 = {"d1","g2"};
		DBApp.select("s2s", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("s2s");
	
		String[] ConditionColumns04 = {"f","d","c"};
		String[] ConditionColumnsValues04 = {"f1","d1","c1"};
		DBApp.select("s2s", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("s2s");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 453);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 446"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("zo995", cols1);
		String [][] records_zo995 = new String[391][cols1.length];
		for(int i=0;i<391;i++)
		{
			records_zo995[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zo995[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zo995", records_zo995[i]);
		}
	
		int pageCount1 = (int)Math.ceil(391.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("zo995", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("zo995");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("zo995", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("zo995", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c1"};
		DBApp.select("zo995", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"c"};
		String[] ConditionColumnsValues15 = {"c0"};
		DBApp.select("zo995", ConditionColumns15, ConditionColumnsValues15);
	
		String selectTrace1 = DBApp.getFullTrace("zo995");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 399);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 391"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("i6e", cols0);
		String [][] records_i6e = new String[316][cols0.length];
		for(int i=0;i<316;i++)
		{
			records_i6e[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i6e[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i6e", records_i6e[i]);
		}
	
		int pageCount0 = (int)Math.ceil(316.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("i6e");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"e"};
		String[] ConditionColumnsValues04 = {"e0"};
		DBApp.select("i6e", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"b"};
		String[] ConditionColumnsValues05 = {"b1"};
		DBApp.select("i6e", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"d"};
		String[] ConditionColumnsValues06 = {"d1"};
		DBApp.select("i6e", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"a","b","m","i"};
		String[] ConditionColumnsValues07 = {"a311","b1","m12","i5"};
		DBApp.select("i6e", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"g"};
		String[] ConditionColumnsValues08 = {"g0"};
		DBApp.select("i6e", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"a","i","j","e","b"};
		String[] ConditionColumnsValues010 = {"a72","i0","j2","e2","b0"};
		DBApp.select("i6e", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("i6e");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"b"};
		String[] ConditionColumnsValues014 = {"b0"};
		DBApp.select("i6e", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"e","l","n","o","i","g"};
		String[] ConditionColumnsValues016 = {"e4","l5","n5","o14","i8","g5"};
		DBApp.select("i6e", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"n"};
		String[] ConditionColumnsValues017 = {"n3"};
		DBApp.select("i6e", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"l"};
		String[] ConditionColumnsValues018 = {"l5"};
		DBApp.select("i6e", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((316%DBApp.dataPageSize))-1;
		DBApp.select("i6e", pageSelectPointer019,recordSelectPointer019);
	
		String selectTrace0 = DBApp.getFullTrace("i6e");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 338);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 316"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("eq", cols0);
		String [][] records_eq = new String[142][cols0.length];
		for(int i=0;i<142;i++)
		{
			records_eq[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_eq[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("eq", records_eq[i]);
		}
	
		int pageCount0 = (int)Math.ceil(142.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns00 = {"m"};
		String[] ConditionColumnsValues00 = {"m5"};
		DBApp.select("eq", ConditionColumns00, ConditionColumnsValues00);
	
		String selectTrace0 = DBApp.getFullTrace("eq");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 145);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 142"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s72q", cols1);
		String [][] records_s72q = new String[336][cols1.length];
		for(int i=0;i<336;i++)
		{
			records_s72q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s72q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s72q", records_s72q[i]);
		}
	
		int pageCount1 = (int)Math.ceil(336.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		DBApp.select("s72q");
	
		DBApp.select("s72q");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("s72q");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"e"};
		String[] ConditionColumnsValues16 = {"e0"};
		DBApp.select("s72q", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("s72q");
	
		String[] ConditionColumns19 = {"i","d"};
		String[] ConditionColumnsValues19 = {"i7","d1"};
		DBApp.select("s72q", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"h"};
		String[] ConditionColumnsValues110 = {"h4"};
		DBApp.select("s72q", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"d"};
		String[] ConditionColumnsValues111 = {"d3"};
		DBApp.select("s72q", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("s72q");
	
		DBApp.select("s72q");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("s72q");
	
		String[] ConditionColumns116 = {"f"};
		String[] ConditionColumnsValues116 = {"f5"};
		DBApp.select("s72q", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("s72q");
	
		String[] ConditionColumns118 = {"g"};
		String[] ConditionColumnsValues118 = {"g5"};
		DBApp.select("s72q", ConditionColumns118, ConditionColumnsValues118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer119,recordSelectPointer119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer120,recordSelectPointer120);
	
		DBApp.select("s72q");
	
		String[] ConditionColumns122 = {"j","g","h","b"};
		String[] ConditionColumnsValues122 = {"j6","g6","h2","b0"};
		DBApp.select("s72q", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"c"};
		String[] ConditionColumnsValues123 = {"c1"};
		DBApp.select("s72q", ConditionColumns123, ConditionColumnsValues123);
	
		DBApp.select("s72q");
	
		int pageSelectPointer125 = genRandNum(pageCount1)-1;
		int recordSelectPointer125 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer125==pageCount1-1)
			recordSelectPointer125 = genRandNum((336%DBApp.dataPageSize))-1;
		DBApp.select("s72q", pageSelectPointer125,recordSelectPointer125);
	
		String selectTrace1 = DBApp.getFullTrace("s72q");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 364);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 336"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e1", cols0);
		String [][] records_e1 = new String[372][cols0.length];
		for(int i=0;i<372;i++)
		{
			records_e1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e1", records_e1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(372.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns00 = {"h"};
		String[] ConditionColumnsValues00 = {"h0"};
		DBApp.select("e1", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("e1");
	
		String[] ConditionColumns02 = {"d"};
		String[] ConditionColumnsValues02 = {"d1"};
		DBApp.select("e1", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"i","c","b","d"};
		String[] ConditionColumnsValues03 = {"i2","c2","b1","d1"};
		DBApp.select("e1", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("e1");
	
		String[] ConditionColumns05 = {"b"};
		String[] ConditionColumnsValues05 = {"b1"};
		DBApp.select("e1", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"f"};
		String[] ConditionColumnsValues010 = {"f4"};
		DBApp.select("e1", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("e1");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d0"};
		DBApp.select("e1", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("e1");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("e1");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"a"};
		String[] ConditionColumnsValues018 = {"a290"};
		DBApp.select("e1", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("e1");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((372%DBApp.dataPageSize))-1;
		DBApp.select("e1", pageSelectPointer020,recordSelectPointer020);
	
		String[] ConditionColumns021 = {"b","g","f","i"};
		String[] ConditionColumnsValues021 = {"b1","g5","f5","i8"};
		DBApp.select("e1", ConditionColumns021, ConditionColumnsValues021);
	
		String selectTrace0 = DBApp.getFullTrace("e1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 396);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 372"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("b1e", cols1);
		String [][] records_b1e = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			records_b1e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b1e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b1e", records_b1e[i]);
		}
	
		int pageCount1 = (int)Math.ceil(70.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer10,recordSelectPointer10);
	
		String[] ConditionColumns11 = {"c"};
		String[] ConditionColumnsValues11 = {"c0"};
		DBApp.select("b1e", ConditionColumns11, ConditionColumnsValues11);
	
		DBApp.select("b1e");
	
		String[] ConditionColumns13 = {"c"};
		String[] ConditionColumnsValues13 = {"c0"};
		DBApp.select("b1e", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("b1e");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("b1e");
	
		DBApp.select("b1e");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((70%DBApp.dataPageSize))-1;
		DBApp.select("b1e", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"a"};
		String[] ConditionColumnsValues111 = {"a19"};
		DBApp.select("b1e", ConditionColumns111, ConditionColumnsValues111);
	
		String selectTrace1 = DBApp.getFullTrace("b1e");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 84);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 70"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y3f", cols0);
		String [][] records_y3f = new String[114][cols0.length];
		for(int i=0;i<114;i++)
		{
			records_y3f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y3f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y3f", records_y3f[i]);
		}
	
		int pageCount0 = (int)Math.ceil(114.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("y3f");
	
		String[] ConditionColumns01 = {"d"};
		String[] ConditionColumnsValues01 = {"d0"};
		DBApp.select("y3f", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer02,recordSelectPointer02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("y3f");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c0"};
		DBApp.select("y3f", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"h"};
		String[] ConditionColumnsValues08 = {"h1"};
		DBApp.select("y3f", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("y3f");
	
		String[] ConditionColumns010 = {"c","g","h"};
		String[] ConditionColumnsValues010 = {"c0","g3","h7"};
		DBApp.select("y3f", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("y3f");
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("y3f");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer015,recordSelectPointer015);
	
		DBApp.select("y3f");
	
		DBApp.select("y3f");
	
		DBApp.select("y3f");
	
		DBApp.select("y3f");
	
		DBApp.select("y3f");
	
		DBApp.select("y3f");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer022,recordSelectPointer022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("y3f");
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer025,recordSelectPointer025);
	
		DBApp.select("y3f");
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((114%DBApp.dataPageSize))-1;
		DBApp.select("y3f", pageSelectPointer027,recordSelectPointer027);
	
		String[] ConditionColumns028 = {"g"};
		String[] ConditionColumnsValues028 = {"g6"};
		DBApp.select("y3f", ConditionColumns028, ConditionColumnsValues028);
	
		String selectTrace0 = DBApp.getFullTrace("y3f");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 145);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 114"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("fr", cols0);
		String [][] records_fr = new String[481][cols0.length];
		for(int i=0;i<481;i++)
		{
			records_fr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fr", records_fr[i]);
		}
	
		int pageCount0 = (int)Math.ceil(481.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a249"};
		DBApp.select("fr", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("fr");
	
		DBApp.select("fr");
	
		String[] ConditionColumns03 = {"d","k","f","j","b"};
		String[] ConditionColumnsValues03 = {"d0","k2","f2","j0","b0"};
		DBApp.select("fr", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"j","i","e","k","h"};
		String[] ConditionColumnsValues04 = {"j5","i5","e0","k9","h1"};
		DBApp.select("fr", ConditionColumns04, ConditionColumnsValues04);
	
		String selectTrace0 = DBApp.getFullTrace("fr");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 488);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 481"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("r980", cols1);
		String [][] records_r980 = new String[148][cols1.length];
		for(int i=0;i<148;i++)
		{
			records_r980[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_r980[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("r980", records_r980[i]);
		}
	
		int pageCount1 = (int)Math.ceil(148.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("r980");
	
		DBApp.select("r980");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"i","j","g"};
		String[] ConditionColumnsValues15 = {"i3","j3","g2"};
		DBApp.select("r980", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"i"};
		String[] ConditionColumnsValues16 = {"i5"};
		DBApp.select("r980", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"g","c","i"};
		String[] ConditionColumnsValues110 = {"g1","c0","i3"};
		DBApp.select("r980", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("r980");
	
		DBApp.select("r980");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer115,recordSelectPointer115);
	
		String[] ConditionColumns116 = {"i"};
		String[] ConditionColumnsValues116 = {"i2"};
		DBApp.select("r980", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"b","h","f"};
		String[] ConditionColumnsValues117 = {"b1","h3","f1"};
		DBApp.select("r980", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"d","h","c","a","i"};
		String[] ConditionColumnsValues118 = {"d0","h0","c1","a112","i4"};
		DBApp.select("r980", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"d"};
		String[] ConditionColumnsValues119 = {"d2"};
		DBApp.select("r980", ConditionColumns119, ConditionColumnsValues119);
	
		String[] ConditionColumns120 = {"f"};
		String[] ConditionColumnsValues120 = {"f5"};
		DBApp.select("r980", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"c"};
		String[] ConditionColumnsValues121 = {"c2"};
		DBApp.select("r980", ConditionColumns121, ConditionColumnsValues121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer122,recordSelectPointer122);
	
		String[] ConditionColumns123 = {"c","h","d","b"};
		String[] ConditionColumnsValues123 = {"c0","h1","d1","b1"};
		DBApp.select("r980", ConditionColumns123, ConditionColumnsValues123);
	
		int pageSelectPointer124 = genRandNum(pageCount1)-1;
		int recordSelectPointer124 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer124==pageCount1-1)
			recordSelectPointer124 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("r980", pageSelectPointer124,recordSelectPointer124);
	
		String[] ConditionColumns125 = {"d"};
		String[] ConditionColumnsValues125 = {"d3"};
		DBApp.select("r980", ConditionColumns125, ConditionColumnsValues125);
	
		DBApp.select("r980");
	
		String selectTrace1 = DBApp.getFullTrace("r980");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 177);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 148"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wr7v", cols2);
		String [][] records_wr7v = new String[127][cols2.length];
		for(int i=0;i<127;i++)
		{
			records_wr7v[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wr7v[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wr7v", records_wr7v[i]);
		}
	
		int pageCount2 = (int)Math.ceil(127.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		DBApp.select("wr7v");
	
		String[] ConditionColumns21 = {"n","e","m","f","d","i"};
		String[] ConditionColumnsValues21 = {"n7","e2","m12","f5","d1","i5"};
		DBApp.select("wr7v", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("wr7v");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((127%DBApp.dataPageSize))-1;
		DBApp.select("wr7v", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("wr7v");
	
		String[] ConditionColumns25 = {"m"};
		String[] ConditionColumnsValues25 = {"m4"};
		DBApp.select("wr7v", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"m","g","n"};
		String[] ConditionColumnsValues26 = {"m10","g5","n5"};
		DBApp.select("wr7v", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((127%DBApp.dataPageSize))-1;
		DBApp.select("wr7v", pageSelectPointer27,recordSelectPointer27);
	
		String selectTrace2 = DBApp.getFullTrace("wr7v");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 137);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 127"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y9y", cols3);
		String [][] records_y9y = new String[238][cols3.length];
		for(int i=0;i<238;i++)
		{
			records_y9y[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y9y[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y9y", records_y9y[i]);
		}
	
		int pageCount3 = (int)Math.ceil(238.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		String[] ConditionColumns30 = {"i"};
		String[] ConditionColumnsValues30 = {"i4"};
		DBApp.select("y9y", ConditionColumns30, ConditionColumnsValues30);
	
		String[] ConditionColumns31 = {"e","j"};
		String[] ConditionColumnsValues31 = {"e0","j5"};
		DBApp.select("y9y", ConditionColumns31, ConditionColumnsValues31);
	
		DBApp.select("y9y");
	
		String[] ConditionColumns33 = {"j"};
		String[] ConditionColumnsValues33 = {"j2"};
		DBApp.select("y9y", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer34,recordSelectPointer34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer35,recordSelectPointer35);
	
		String[] ConditionColumns36 = {"h"};
		String[] ConditionColumnsValues36 = {"h3"};
		DBApp.select("y9y", ConditionColumns36, ConditionColumnsValues36);
	
		String[] ConditionColumns37 = {"d"};
		String[] ConditionColumnsValues37 = {"d0"};
		DBApp.select("y9y", ConditionColumns37, ConditionColumnsValues37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("y9y");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("y9y");
	
		String[] ConditionColumns312 = {"c"};
		String[] ConditionColumnsValues312 = {"c2"};
		DBApp.select("y9y", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer314,recordSelectPointer314);
	
		DBApp.select("y9y");
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer316,recordSelectPointer316);
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((238%DBApp.dataPageSize))-1;
		DBApp.select("y9y", pageSelectPointer317,recordSelectPointer317);
	
		String[] ConditionColumns318 = {"e"};
		String[] ConditionColumnsValues318 = {"e1"};
		DBApp.select("y9y", ConditionColumns318, ConditionColumnsValues318);
	
		String[] ConditionColumns319 = {"g"};
		String[] ConditionColumnsValues319 = {"g0"};
		DBApp.select("y9y", ConditionColumns319, ConditionColumnsValues319);
	
		DBApp.select("y9y");
	
		String selectTrace3 = DBApp.getFullTrace("y9y");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 261);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 238"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("c1", cols0);
		String [][] records_c1 = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			records_c1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_c1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("c1", records_c1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(71.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		DBApp.select("c1");
	
		DBApp.select("c1");
	
		DBApp.select("c1");
	
		String selectTrace0 = DBApp.getFullTrace("c1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 76);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 71"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("y4wr", cols1);
		String [][] records_y4wr = new String[427][cols1.length];
		for(int i=0;i<427;i++)
		{
			records_y4wr[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y4wr[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y4wr", records_y4wr[i]);
		}
	
		int pageCount1 = (int)Math.ceil(427.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		DBApp.select("y4wr");
	
		DBApp.select("y4wr");
	
		String[] ConditionColumns12 = {"d"};
		String[] ConditionColumnsValues12 = {"d3"};
		DBApp.select("y4wr", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"c"};
		String[] ConditionColumnsValues13 = {"c2"};
		DBApp.select("y4wr", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c","d"};
		String[] ConditionColumnsValues14 = {"c2","d2"};
		DBApp.select("y4wr", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("y4wr");
	
		DBApp.select("y4wr");
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("y4wr", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("y4wr", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("y4wr", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("y4wr");
	
		DBApp.select("y4wr");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((427%DBApp.dataPageSize))-1;
		DBApp.select("y4wr", pageSelectPointer112,recordSelectPointer112);
	
		String selectTrace1 = DBApp.getFullTrace("y4wr");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 442);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 427"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k05u0", cols2);
		String [][] records_k05u0 = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			records_k05u0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_k05u0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("k05u0", records_k05u0[i]);
		}
	
		int pageCount2 = (int)Math.ceil(15.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((15%DBApp.dataPageSize))-1;
		DBApp.select("k05u0", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("k05u0");
	
		DBApp.select("k05u0");
	
		DBApp.select("k05u0");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((15%DBApp.dataPageSize))-1;
		DBApp.select("k05u0", pageSelectPointer24,recordSelectPointer24);
	
		String selectTrace2 = DBApp.getFullTrace("k05u0");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 22);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 15"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("sve", cols3);
		String [][] records_sve = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			records_sve[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_sve[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("sve", records_sve[i]);
		}
	
		int pageCount3 = (int)Math.ceil(73.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"f","e"};
		String[] ConditionColumnsValues31 = {"f1","e3"};
		DBApp.select("sve", ConditionColumns31, ConditionColumnsValues31);
	
		DBApp.select("sve");
	
		String[] ConditionColumns33 = {"b"};
		String[] ConditionColumnsValues33 = {"b0"};
		DBApp.select("sve", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("sve");
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer36,recordSelectPointer36);
	
		String[] ConditionColumns37 = {"e"};
		String[] ConditionColumnsValues37 = {"e4"};
		DBApp.select("sve", ConditionColumns37, ConditionColumnsValues37);
	
		String[] ConditionColumns38 = {"b"};
		String[] ConditionColumnsValues38 = {"b0"};
		DBApp.select("sve", ConditionColumns38, ConditionColumnsValues38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer39,recordSelectPointer39);
	
		String[] ConditionColumns310 = {"c","b"};
		String[] ConditionColumnsValues310 = {"c1","b0"};
		DBApp.select("sve", ConditionColumns310, ConditionColumnsValues310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer311,recordSelectPointer311);
	
		String[] ConditionColumns312 = {"c","d"};
		String[] ConditionColumnsValues312 = {"c0","d3"};
		DBApp.select("sve", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"a"};
		String[] ConditionColumnsValues313 = {"a22"};
		DBApp.select("sve", ConditionColumns313, ConditionColumnsValues313);
	
		DBApp.select("sve");
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer315,recordSelectPointer315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer316,recordSelectPointer316);
	
		String[] ConditionColumns317 = {"d"};
		String[] ConditionColumnsValues317 = {"d2"};
		DBApp.select("sve", ConditionColumns317, ConditionColumnsValues317);
	
		String[] ConditionColumns318 = {"e"};
		String[] ConditionColumnsValues318 = {"e0"};
		DBApp.select("sve", ConditionColumns318, ConditionColumnsValues318);
	
		String[] ConditionColumns319 = {"c"};
		String[] ConditionColumnsValues319 = {"c0"};
		DBApp.select("sve", ConditionColumns319, ConditionColumnsValues319);
	
		DBApp.select("sve");
	
		String[] ConditionColumns321 = {"d"};
		String[] ConditionColumnsValues321 = {"d3"};
		DBApp.select("sve", ConditionColumns321, ConditionColumnsValues321);
	
		String[] ConditionColumns322 = {"c"};
		String[] ConditionColumnsValues322 = {"c1"};
		DBApp.select("sve", ConditionColumns322, ConditionColumnsValues322);
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer323,recordSelectPointer323);
	
		int pageSelectPointer324 = genRandNum(pageCount3)-1;
		int recordSelectPointer324 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer324==pageCount3-1)
			recordSelectPointer324 = genRandNum((73%DBApp.dataPageSize))-1;
		DBApp.select("sve", pageSelectPointer324,recordSelectPointer324);
	
		String selectTrace3 = DBApp.getFullTrace("sve");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 100);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 73"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o4q", cols4);
		String [][] records_o4q = new String[464][cols4.length];
		for(int i=0;i<464;i++)
		{
			records_o4q[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o4q[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o4q", records_o4q[i]);
		}
	
		int pageCount4 = (int)Math.ceil(464.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns40 = {"c"};
		String[] ConditionColumnsValues40 = {"c0"};
		DBApp.select("o4q", ConditionColumns40, ConditionColumnsValues40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer41,recordSelectPointer41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer42,recordSelectPointer42);
	
		DBApp.select("o4q");
	
		String[] ConditionColumns44 = {"a","f","d","j","e"};
		String[] ConditionColumnsValues44 = {"a3","f3","d3","j3","e3"};
		DBApp.select("o4q", ConditionColumns44, ConditionColumnsValues44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer45,recordSelectPointer45);
	
		String[] ConditionColumns46 = {"c"};
		String[] ConditionColumnsValues46 = {"c2"};
		DBApp.select("o4q", ConditionColumns46, ConditionColumnsValues46);
	
		DBApp.select("o4q");
	
		int pageSelectPointer48 = genRandNum(pageCount4)-1;
		int recordSelectPointer48 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer48==pageCount4-1)
			recordSelectPointer48 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer48,recordSelectPointer48);
	
		int pageSelectPointer49 = genRandNum(pageCount4)-1;
		int recordSelectPointer49 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer49==pageCount4-1)
			recordSelectPointer49 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer49,recordSelectPointer49);
	
		String[] ConditionColumns410 = {"g"};
		String[] ConditionColumnsValues410 = {"g3"};
		DBApp.select("o4q", ConditionColumns410, ConditionColumnsValues410);
	
		String[] ConditionColumns411 = {"e"};
		String[] ConditionColumnsValues411 = {"e3"};
		DBApp.select("o4q", ConditionColumns411, ConditionColumnsValues411);
	
		int pageSelectPointer412 = genRandNum(pageCount4)-1;
		int recordSelectPointer412 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer412==pageCount4-1)
			recordSelectPointer412 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer412,recordSelectPointer412);
	
		String[] ConditionColumns413 = {"f"};
		String[] ConditionColumnsValues413 = {"f2"};
		DBApp.select("o4q", ConditionColumns413, ConditionColumnsValues413);
	
		DBApp.select("o4q");
	
		DBApp.select("o4q");
	
		DBApp.select("o4q");
	
		DBApp.select("o4q");
	
		DBApp.select("o4q");
	
		DBApp.select("o4q");
	
		int pageSelectPointer420 = genRandNum(pageCount4)-1;
		int recordSelectPointer420 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer420==pageCount4-1)
			recordSelectPointer420 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer420,recordSelectPointer420);
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer421,recordSelectPointer421);
	
		int pageSelectPointer422 = genRandNum(pageCount4)-1;
		int recordSelectPointer422 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer422==pageCount4-1)
			recordSelectPointer422 = genRandNum((464%DBApp.dataPageSize))-1;
		DBApp.select("o4q", pageSelectPointer422,recordSelectPointer422);
	
		String selectTrace4 = DBApp.getFullTrace("o4q");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 489);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 464"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("xan", cols0);
		String [][] records_xan = new String[344][cols0.length];
		for(int i=0;i<344;i++)
		{
			records_xan[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xan[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xan", records_xan[i]);
		}
	
		int pageCount0 = (int)Math.ceil(344.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns00 = {"c"};
		String[] ConditionColumnsValues00 = {"c0"};
		DBApp.select("xan", ConditionColumns00, ConditionColumnsValues00);
	
		String selectTrace0 = DBApp.getFullTrace("xan");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 347);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 344"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("xfo", cols1);
		String [][] records_xfo = new String[202][cols1.length];
		for(int i=0;i<202;i++)
		{
			records_xfo[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_xfo[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("xfo", records_xfo[i]);
		}
	
		int pageCount1 = (int)Math.ceil(202.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("xfo");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"d","a"};
		String[] ConditionColumnsValues15 = {"d2","a14"};
		DBApp.select("xfo", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"b","f","e"};
		String[] ConditionColumnsValues16 = {"b1","f5","e1"};
		DBApp.select("xfo", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("xfo");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"b"};
		String[] ConditionColumnsValues19 = {"b1"};
		DBApp.select("xfo", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer110,recordSelectPointer110);
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b0"};
		DBApp.select("xfo", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("xfo");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((202%DBApp.dataPageSize))-1;
		DBApp.select("xfo", pageSelectPointer119,recordSelectPointer119);
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		String[] ConditionColumns123 = {"b","a"};
		String[] ConditionColumnsValues123 = {"b0","a6"};
		DBApp.select("xfo", ConditionColumns123, ConditionColumnsValues123);
	
		DBApp.select("xfo");
	
		DBApp.select("xfo");
	
		String[] ConditionColumns126 = {"d"};
		String[] ConditionColumnsValues126 = {"d3"};
		DBApp.select("xfo", ConditionColumns126, ConditionColumnsValues126);
	
		String selectTrace1 = DBApp.getFullTrace("xfo");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 231);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 202"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h497", cols2);
		String [][] records_h497 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			records_h497[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h497[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h497", records_h497[i]);
		}
	
		int pageCount2 = (int)Math.ceil(48.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		DBApp.select("h497");
	
		String[] ConditionColumns21 = {"f"};
		String[] ConditionColumnsValues21 = {"f3"};
		DBApp.select("h497", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer22,recordSelectPointer22);
	
		String[] ConditionColumns23 = {"g"};
		String[] ConditionColumnsValues23 = {"g4"};
		DBApp.select("h497", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"b"};
		String[] ConditionColumnsValues24 = {"b0"};
		DBApp.select("h497", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("h497");
	
		DBApp.select("h497");
	
		DBApp.select("h497");
	
		String[] ConditionColumns29 = {"g"};
		String[] ConditionColumnsValues29 = {"g5"};
		DBApp.select("h497", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"c"};
		String[] ConditionColumnsValues211 = {"c1"};
		DBApp.select("h497", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"f"};
		String[] ConditionColumnsValues212 = {"f0"};
		DBApp.select("h497", ConditionColumns212, ConditionColumnsValues212);
	
		String[] ConditionColumns213 = {"c","e","d"};
		String[] ConditionColumnsValues213 = {"c0","e3","d2"};
		DBApp.select("h497", ConditionColumns213, ConditionColumnsValues213);
	
		String[] ConditionColumns214 = {"c"};
		String[] ConditionColumnsValues214 = {"c1"};
		DBApp.select("h497", ConditionColumns214, ConditionColumnsValues214);
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer215,recordSelectPointer215);
	
		String[] ConditionColumns216 = {"e","c","d"};
		String[] ConditionColumnsValues216 = {"e0","c0","d1"};
		DBApp.select("h497", ConditionColumns216, ConditionColumnsValues216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer217,recordSelectPointer217);
	
		String[] ConditionColumns218 = {"b"};
		String[] ConditionColumnsValues218 = {"b1"};
		DBApp.select("h497", ConditionColumns218, ConditionColumnsValues218);
	
		String[] ConditionColumns219 = {"g"};
		String[] ConditionColumnsValues219 = {"g4"};
		DBApp.select("h497", ConditionColumns219, ConditionColumnsValues219);
	
		DBApp.select("h497");
	
		DBApp.select("h497");
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer222,recordSelectPointer222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer223,recordSelectPointer223);
	
		String[] ConditionColumns224 = {"f","c","e"};
		String[] ConditionColumnsValues224 = {"f2","c2","e4"};
		DBApp.select("h497", ConditionColumns224, ConditionColumnsValues224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((48%DBApp.dataPageSize))-1;
		DBApp.select("h497", pageSelectPointer225,recordSelectPointer225);
	
		String[] ConditionColumns226 = {"d"};
		String[] ConditionColumnsValues226 = {"d2"};
		DBApp.select("h497", ConditionColumns226, ConditionColumnsValues226);
	
		String selectTrace2 = DBApp.getFullTrace("h497");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 77);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 48"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("g5hu", cols3);
		String [][] records_g5hu = new String[485][cols3.length];
		for(int i=0;i<485;i++)
		{
			records_g5hu[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_g5hu[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("g5hu", records_g5hu[i]);
		}
	
		int pageCount3 = (int)Math.ceil(485.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		DBApp.select("g5hu");
	
		DBApp.select("g5hu");
	
		String[] ConditionColumns32 = {"b"};
		String[] ConditionColumnsValues32 = {"b1"};
		DBApp.select("g5hu", ConditionColumns32, ConditionColumnsValues32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"d"};
		String[] ConditionColumnsValues35 = {"d2"};
		DBApp.select("g5hu", ConditionColumns35, ConditionColumnsValues35);
	
		String[] ConditionColumns36 = {"d"};
		String[] ConditionColumnsValues36 = {"d1"};
		DBApp.select("g5hu", ConditionColumns36, ConditionColumnsValues36);
	
		DBApp.select("g5hu");
	
		DBApp.select("g5hu");
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("g5hu");
	
		String[] ConditionColumns311 = {"d"};
		String[] ConditionColumnsValues311 = {"d2"};
		DBApp.select("g5hu", ConditionColumns311, ConditionColumnsValues311);
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer312,recordSelectPointer312);
	
		DBApp.select("g5hu");
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"b"};
		String[] ConditionColumnsValues315 = {"b0"};
		DBApp.select("g5hu", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("g5hu");
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer317,recordSelectPointer317);
	
		DBApp.select("g5hu");
	
		DBApp.select("g5hu");
	
		DBApp.select("g5hu");
	
		DBApp.select("g5hu");
	
		DBApp.select("g5hu");
	
		String[] ConditionColumns323 = {"d"};
		String[] ConditionColumnsValues323 = {"d3"};
		DBApp.select("g5hu", ConditionColumns323, ConditionColumnsValues323);
	
		String[] ConditionColumns324 = {"b"};
		String[] ConditionColumnsValues324 = {"b1"};
		DBApp.select("g5hu", ConditionColumns324, ConditionColumnsValues324);
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("g5hu", pageSelectPointer325,recordSelectPointer325);
	
		DBApp.select("g5hu");
	
		String selectTrace3 = DBApp.getFullTrace("g5hu");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 514);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 485"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("i8g5f", cols0);
		String [][] records_i8g5f = new String[462][cols0.length];
		for(int i=0;i<462;i++)
		{
			records_i8g5f[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i8g5f[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i8g5f", records_i8g5f[i]);
		}
	
		int pageCount0 = (int)Math.ceil(462.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		DBApp.select("i8g5f");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"i"};
		String[] ConditionColumnsValues03 = {"i5"};
		DBApp.select("i8g5f", ConditionColumns03, ConditionColumnsValues03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"j"};
		String[] ConditionColumnsValues05 = {"j8"};
		DBApp.select("i8g5f", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"h"};
		String[] ConditionColumnsValues07 = {"h3"};
		DBApp.select("i8g5f", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("i8g5f");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer09,recordSelectPointer09);
	
		DBApp.select("i8g5f");
	
		DBApp.select("i8g5f");
	
		String[] ConditionColumns012 = {"j","d","h","f","k"};
		String[] ConditionColumnsValues012 = {"j6","d0","h0","f2","k10"};
		DBApp.select("i8g5f", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("i8g5f");
	
		String[] ConditionColumns015 = {"j","i","d","k"};
		String[] ConditionColumnsValues015 = {"j1","i7","d1","k3"};
		DBApp.select("i8g5f", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((462%DBApp.dataPageSize))-1;
		DBApp.select("i8g5f", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"e","i","g","c","a"};
		String[] ConditionColumnsValues017 = {"e4","i8","g2","c2","a44"};
		DBApp.select("i8g5f", ConditionColumns017, ConditionColumnsValues017);
	
		String selectTrace0 = DBApp.getFullTrace("i8g5f");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 482);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 462"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("fp1k", cols1);
		String [][] records_fp1k = new String[435][cols1.length];
		for(int i=0;i<435;i++)
		{
			records_fp1k[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_fp1k[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("fp1k", records_fp1k[i]);
		}
	
		int pageCount1 = (int)Math.ceil(435.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		String[] ConditionColumns10 = {"g"};
		String[] ConditionColumnsValues10 = {"g0"};
		DBApp.select("fp1k", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((435%DBApp.dataPageSize))-1;
		DBApp.select("fp1k", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((435%DBApp.dataPageSize))-1;
		DBApp.select("fp1k", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((435%DBApp.dataPageSize))-1;
		DBApp.select("fp1k", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("fp1k");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((435%DBApp.dataPageSize))-1;
		DBApp.select("fp1k", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b1"};
		DBApp.select("fp1k", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("fp1k");
	
		String[] ConditionColumns18 = {"c","g","h"};
		String[] ConditionColumnsValues18 = {"c1","g0","h4"};
		DBApp.select("fp1k", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"h","f"};
		String[] ConditionColumnsValues19 = {"h2","f0"};
		DBApp.select("fp1k", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"b","h","e"};
		String[] ConditionColumnsValues110 = {"b0","h4","e0"};
		DBApp.select("fp1k", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("fp1k");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((435%DBApp.dataPageSize))-1;
		DBApp.select("fp1k", pageSelectPointer112,recordSelectPointer112);
	
		String selectTrace1 = DBApp.getFullTrace("fp1k");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 450);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 435"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("s2", cols2);
		String [][] records_s2 = new String[289][cols2.length];
		for(int i=0;i<289;i++)
		{
			records_s2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_s2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("s2", records_s2[i]);
		}
	
		int pageCount2 = (int)Math.ceil(289.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		DBApp.select("s2");
	
		DBApp.select("s2");
	
		String[] ConditionColumns22 = {"d","b","h","k"};
		String[] ConditionColumnsValues22 = {"d2","b0","h6","k3"};
		DBApp.select("s2", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("s2");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"c"};
		String[] ConditionColumnsValues25 = {"c2"};
		DBApp.select("s2", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"h"};
		String[] ConditionColumnsValues26 = {"h6"};
		DBApp.select("s2", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"b"};
		String[] ConditionColumnsValues29 = {"b0"};
		DBApp.select("s2", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("s2");
	
		String[] ConditionColumns211 = {"c"};
		String[] ConditionColumnsValues211 = {"c2"};
		DBApp.select("s2", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"m","i","h"};
		String[] ConditionColumnsValues212 = {"m4","i4","h6"};
		DBApp.select("s2", ConditionColumns212, ConditionColumnsValues212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer213,recordSelectPointer213);
	
		String[] ConditionColumns214 = {"l","m"};
		String[] ConditionColumnsValues214 = {"l4","m8"};
		DBApp.select("s2", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"e"};
		String[] ConditionColumnsValues215 = {"e2"};
		DBApp.select("s2", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("s2");
	
		DBApp.select("s2");
	
		String[] ConditionColumns218 = {"c","a","d","l","e"};
		String[] ConditionColumnsValues218 = {"c0","a108","d0","l0","e3"};
		DBApp.select("s2", ConditionColumns218, ConditionColumnsValues218);
	
		DBApp.select("s2");
	
		String[] ConditionColumns220 = {"i","m","a","c"};
		String[] ConditionColumnsValues220 = {"i4","m0","a13","c1"};
		DBApp.select("s2", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"f"};
		String[] ConditionColumnsValues221 = {"f2"};
		DBApp.select("s2", ConditionColumns221, ConditionColumnsValues221);
	
		String[] ConditionColumns222 = {"f","d","l"};
		String[] ConditionColumnsValues222 = {"f4","d2","l10"};
		DBApp.select("s2", ConditionColumns222, ConditionColumnsValues222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("s2", pageSelectPointer223,recordSelectPointer223);
	
		String selectTrace2 = DBApp.getFullTrace("s2");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 315);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 289"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("sy6y", cols0);
		String [][] records_sy6y = new String[131][cols0.length];
		for(int i=0;i<131;i++)
		{
			records_sy6y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_sy6y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("sy6y", records_sy6y[i]);
		}
	
		int pageCount0 = (int)Math.ceil(131.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		String[] ConditionColumns00 = {"e"};
		String[] ConditionColumnsValues00 = {"e2"};
		DBApp.select("sy6y", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"n"};
		String[] ConditionColumnsValues01 = {"n8"};
		DBApp.select("sy6y", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"n","b","k"};
		String[] ConditionColumnsValues02 = {"n4","b0","k6"};
		DBApp.select("sy6y", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("sy6y");
	
		String[] ConditionColumns04 = {"c"};
		String[] ConditionColumnsValues04 = {"c0"};
		DBApp.select("sy6y", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("sy6y");
	
		String[] ConditionColumns06 = {"c"};
		String[] ConditionColumnsValues06 = {"c0"};
		DBApp.select("sy6y", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("sy6y");
	
		DBApp.select("sy6y");
	
		String selectTrace0 = DBApp.getFullTrace("sy6y");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 142);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 131"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("h3", cols1);
		String [][] records_h3 = new String[433][cols1.length];
		for(int i=0;i<433;i++)
		{
			records_h3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h3", records_h3[i]);
		}
	
		int pageCount1 = (int)Math.ceil(433.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		String[] ConditionColumns10 = {"d"};
		String[] ConditionColumnsValues10 = {"d1"};
		DBApp.select("h3", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"d"};
		String[] ConditionColumnsValues11 = {"d1"};
		DBApp.select("h3", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"c"};
		String[] ConditionColumnsValues12 = {"c1"};
		DBApp.select("h3", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a309"};
		DBApp.select("h3", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c0"};
		DBApp.select("h3", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((433%DBApp.dataPageSize))-1;
		DBApp.select("h3", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("h3");
	
		DBApp.select("h3");
	
		DBApp.select("h3");
	
		DBApp.select("h3");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((433%DBApp.dataPageSize))-1;
		DBApp.select("h3", pageSelectPointer110,recordSelectPointer110);
	
		String selectTrace1 = DBApp.getFullTrace("h3");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 446);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 433"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("kzx", cols2);
		String [][] records_kzx = new String[306][cols2.length];
		for(int i=0;i<306;i++)
		{
			records_kzx[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kzx[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kzx", records_kzx[i]);
		}
	
		int pageCount2 = (int)Math.ceil(306.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("kzx");
	
		String[] ConditionColumns21 = {"b"};
		String[] ConditionColumnsValues21 = {"b1"};
		DBApp.select("kzx", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((306%DBApp.dataPageSize))-1;
		DBApp.select("kzx", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((306%DBApp.dataPageSize))-1;
		DBApp.select("kzx", pageSelectPointer23,recordSelectPointer23);
	
		DBApp.select("kzx");
	
		String[] ConditionColumns25 = {"b"};
		String[] ConditionColumnsValues25 = {"b0"};
		DBApp.select("kzx", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((306%DBApp.dataPageSize))-1;
		DBApp.select("kzx", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("kzx");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((306%DBApp.dataPageSize))-1;
		DBApp.select("kzx", pageSelectPointer28,recordSelectPointer28);
	
		String[] ConditionColumns29 = {"b"};
		String[] ConditionColumnsValues29 = {"b0"};
		DBApp.select("kzx", ConditionColumns29, ConditionColumnsValues29);
	
		String[] ConditionColumns210 = {"b"};
		String[] ConditionColumnsValues210 = {"b1"};
		DBApp.select("kzx", ConditionColumns210, ConditionColumnsValues210);
	
		DBApp.select("kzx");
	
		DBApp.select("kzx");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((306%DBApp.dataPageSize))-1;
		DBApp.select("kzx", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((306%DBApp.dataPageSize))-1;
		DBApp.select("kzx", pageSelectPointer214,recordSelectPointer214);
	
		String selectTrace2 = DBApp.getFullTrace("kzx");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 323);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 306"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("v1yg2", cols3);
		String [][] records_v1yg2 = new String[250][cols3.length];
		for(int i=0;i<250;i++)
		{
			records_v1yg2[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_v1yg2[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("v1yg2", records_v1yg2[i]);
		}
	
		int pageCount3 = (int)Math.ceil(250.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		String[] ConditionColumns30 = {"c"};
		String[] ConditionColumnsValues30 = {"c2"};
		DBApp.select("v1yg2", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("v1yg2");
	
		DBApp.select("v1yg2");
	
		DBApp.select("v1yg2");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("v1yg2");
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("v1yg2");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer38,recordSelectPointer38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer39,recordSelectPointer39);
	
		DBApp.select("v1yg2");
	
		String[] ConditionColumns311 = {"a"};
		String[] ConditionColumnsValues311 = {"a154"};
		DBApp.select("v1yg2", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"b"};
		String[] ConditionColumnsValues312 = {"b0"};
		DBApp.select("v1yg2", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer314,recordSelectPointer314);
	
		int pageSelectPointer315 = genRandNum(pageCount3)-1;
		int recordSelectPointer315 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer315==pageCount3-1)
			recordSelectPointer315 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer315,recordSelectPointer315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer316,recordSelectPointer316);
	
		DBApp.select("v1yg2");
	
		String[] ConditionColumns318 = {"c"};
		String[] ConditionColumnsValues318 = {"c2"};
		DBApp.select("v1yg2", ConditionColumns318, ConditionColumnsValues318);
	
		DBApp.select("v1yg2");
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer320,recordSelectPointer320);
	
		String[] ConditionColumns321 = {"c"};
		String[] ConditionColumnsValues321 = {"c2"};
		DBApp.select("v1yg2", ConditionColumns321, ConditionColumnsValues321);
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer322,recordSelectPointer322);
	
		DBApp.select("v1yg2");
	
		DBApp.select("v1yg2");
	
		String[] ConditionColumns325 = {"b"};
		String[] ConditionColumnsValues325 = {"b1"};
		DBApp.select("v1yg2", ConditionColumns325, ConditionColumnsValues325);
	
		DBApp.select("v1yg2");
	
		int pageSelectPointer327 = genRandNum(pageCount3)-1;
		int recordSelectPointer327 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer327==pageCount3-1)
			recordSelectPointer327 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer327,recordSelectPointer327);
	
		int pageSelectPointer328 = genRandNum(pageCount3)-1;
		int recordSelectPointer328 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer328==pageCount3-1)
			recordSelectPointer328 = genRandNum((250%DBApp.dataPageSize))-1;
		DBApp.select("v1yg2", pageSelectPointer328,recordSelectPointer328);
	
		DBApp.select("v1yg2");
	
		String selectTrace3 = DBApp.getFullTrace("v1yg2");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 282);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 250"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b"};
		DBApp.createTable("j3", cols4);
		String [][] records_j3 = new String[391][cols4.length];
		for(int i=0;i<391;i++)
		{
			records_j3[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_j3[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("j3", records_j3[i]);
		}
	
		int pageCount4 = (int)Math.ceil(391.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns40 = {"b"};
		String[] ConditionColumnsValues40 = {"b0"};
		DBApp.select("j3", ConditionColumns40, ConditionColumnsValues40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer41,recordSelectPointer41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer42,recordSelectPointer42);
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer43,recordSelectPointer43);
	
		String[] ConditionColumns44 = {"b"};
		String[] ConditionColumnsValues44 = {"b0"};
		DBApp.select("j3", ConditionColumns44, ConditionColumnsValues44);
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer45,recordSelectPointer45);
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		int pageSelectPointer49 = genRandNum(pageCount4)-1;
		int recordSelectPointer49 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer49==pageCount4-1)
			recordSelectPointer49 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer49,recordSelectPointer49);
	
		DBApp.select("j3");
	
		DBApp.select("j3");
	
		String[] ConditionColumns412 = {"b"};
		String[] ConditionColumnsValues412 = {"b1"};
		DBApp.select("j3", ConditionColumns412, ConditionColumnsValues412);
	
		String[] ConditionColumns413 = {"b"};
		String[] ConditionColumnsValues413 = {"b1"};
		DBApp.select("j3", ConditionColumns413, ConditionColumnsValues413);
	
		String[] ConditionColumns414 = {"b"};
		String[] ConditionColumnsValues414 = {"b1"};
		DBApp.select("j3", ConditionColumns414, ConditionColumnsValues414);
	
		DBApp.select("j3");
	
		String[] ConditionColumns416 = {"b"};
		String[] ConditionColumnsValues416 = {"b1"};
		DBApp.select("j3", ConditionColumns416, ConditionColumnsValues416);
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer417,recordSelectPointer417);
	
		int pageSelectPointer418 = genRandNum(pageCount4)-1;
		int recordSelectPointer418 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer418==pageCount4-1)
			recordSelectPointer418 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer418,recordSelectPointer418);
	
		int pageSelectPointer419 = genRandNum(pageCount4)-1;
		int recordSelectPointer419 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer419==pageCount4-1)
			recordSelectPointer419 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer419,recordSelectPointer419);
	
		DBApp.select("j3");
	
		int pageSelectPointer421 = genRandNum(pageCount4)-1;
		int recordSelectPointer421 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer421==pageCount4-1)
			recordSelectPointer421 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("j3", pageSelectPointer421,recordSelectPointer421);
	
		String selectTrace4 = DBApp.getFullTrace("j3");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 415);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 391"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("lmt", cols0);
		String [][] records_lmt = new String[323][cols0.length];
		for(int i=0;i<323;i++)
		{
			records_lmt[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_lmt[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("lmt", records_lmt[i]);
		}
	
		int pageCount0 = (int)Math.ceil(323.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("lmt");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("lmt", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"e"};
		String[] ConditionColumnsValues02 = {"e4"};
		DBApp.select("lmt", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("lmt", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("lmt", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("lmt");
	
		String[] ConditionColumns06 = {"g"};
		String[] ConditionColumnsValues06 = {"g0"};
		DBApp.select("lmt", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("lmt", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("lmt", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"d"};
		String[] ConditionColumnsValues09 = {"d3"};
		DBApp.select("lmt", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("lmt");
	
		String[] ConditionColumns011 = {"f"};
		String[] ConditionColumnsValues011 = {"f2"};
		DBApp.select("lmt", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("lmt");
	
		DBApp.select("lmt");
	
		DBApp.select("lmt");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("lmt", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"g","h","a"};
		String[] ConditionColumnsValues016 = {"g3","h7","a87"};
		DBApp.select("lmt", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("lmt");
	
		DBApp.select("lmt");
	
		DBApp.select("lmt");
	
		String selectTrace0 = DBApp.getFullTrace("lmt");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 345);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 323"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("hm32", cols0);
		String [][] records_hm32 = new String[116][cols0.length];
		for(int i=0;i<116;i++)
		{
			records_hm32[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hm32[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hm32", records_hm32[i]);
		}
	
		int pageCount0 = (int)Math.ceil(116.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns00 = {"c"};
		String[] ConditionColumnsValues00 = {"c1"};
		DBApp.select("hm32", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("hm32");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("hm32");
	
		DBApp.select("hm32");
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a82"};
		DBApp.select("hm32", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("hm32");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("hm32");
	
		DBApp.select("hm32");
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b0"};
		DBApp.select("hm32", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"e"};
		String[] ConditionColumnsValues013 = {"e3"};
		DBApp.select("hm32", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"b"};
		String[] ConditionColumnsValues014 = {"b1"};
		DBApp.select("hm32", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer015,recordSelectPointer015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer016,recordSelectPointer016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer017,recordSelectPointer017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer018,recordSelectPointer018);
	
		String[] ConditionColumns019 = {"c"};
		String[] ConditionColumnsValues019 = {"c0"};
		DBApp.select("hm32", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"d","c"};
		String[] ConditionColumnsValues020 = {"d1","c2"};
		DBApp.select("hm32", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((116%DBApp.dataPageSize))-1;
		DBApp.select("hm32", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("hm32");
	
		String selectTrace0 = DBApp.getFullTrace("hm32");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 141);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 116"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("y3", cols1);
		String [][] records_y3 = new String[148][cols1.length];
		for(int i=0;i<148;i++)
		{
			records_y3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y3", records_y3[i]);
		}
	
		int pageCount1 = (int)Math.ceil(148.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("y3");
	
		String[] ConditionColumns13 = {"a","d"};
		String[] ConditionColumnsValues13 = {"a119","d3"};
		DBApp.select("y3", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("y3");
	
		String[] ConditionColumns15 = {"e"};
		String[] ConditionColumnsValues15 = {"e0"};
		DBApp.select("y3", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b0"};
		DBApp.select("y3", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("y3");
	
		String[] ConditionColumns18 = {"d"};
		String[] ConditionColumnsValues18 = {"d2"};
		DBApp.select("y3", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("y3");
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("y3");
	
		String[] ConditionColumns113 = {"e"};
		String[] ConditionColumnsValues113 = {"e4"};
		DBApp.select("y3", ConditionColumns113, ConditionColumnsValues113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("y3");
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("y3");
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("y3");
	
		String[] ConditionColumns120 = {"d","a"};
		String[] ConditionColumnsValues120 = {"d1","a65"};
		DBApp.select("y3", ConditionColumns120, ConditionColumnsValues120);
	
		String[] ConditionColumns121 = {"c"};
		String[] ConditionColumnsValues121 = {"c0"};
		DBApp.select("y3", ConditionColumns121, ConditionColumnsValues121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((148%DBApp.dataPageSize))-1;
		DBApp.select("y3", pageSelectPointer122,recordSelectPointer122);
	
		DBApp.select("y3");
	
		String selectTrace1 = DBApp.getFullTrace("y3");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 174);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 148"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("v0", cols2);
		String [][] records_v0 = new String[359][cols2.length];
		for(int i=0;i<359;i++)
		{
			records_v0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_v0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("v0", records_v0[i]);
		}
	
		int pageCount2 = (int)Math.ceil(359.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		String[] ConditionColumns20 = {"e"};
		String[] ConditionColumnsValues20 = {"e0"};
		DBApp.select("v0", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"b","e","d"};
		String[] ConditionColumnsValues21 = {"b1","e0","d3"};
		DBApp.select("v0", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("v0");
	
		String[] ConditionColumns23 = {"d"};
		String[] ConditionColumnsValues23 = {"d2"};
		DBApp.select("v0", ConditionColumns23, ConditionColumnsValues23);
	
		DBApp.select("v0");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"a"};
		String[] ConditionColumnsValues26 = {"a349"};
		DBApp.select("v0", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("v0");
	
		String[] ConditionColumns210 = {"a","g","c"};
		String[] ConditionColumnsValues210 = {"a326","g4","c2"};
		DBApp.select("v0", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"b"};
		String[] ConditionColumnsValues211 = {"b1"};
		DBApp.select("v0", ConditionColumns211, ConditionColumnsValues211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((359%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer212,recordSelectPointer212);
	
		String selectTrace2 = DBApp.getFullTrace("v0");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 374);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 359"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h9r", cols0);
		String [][] records_h9r = new String[382][cols0.length];
		for(int i=0;i<382;i++)
		{
			records_h9r[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_h9r[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("h9r", records_h9r[i]);
		}
	
		int pageCount0 = (int)Math.ceil(382.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns00 = {"e","a"};
		String[] ConditionColumnsValues00 = {"e2","a2"};
		DBApp.select("h9r", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("h9r");
	
		DBApp.select("h9r");
	
		String[] ConditionColumns03 = {"d","f"};
		String[] ConditionColumnsValues03 = {"d1","f1"};
		DBApp.select("h9r", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d3"};
		DBApp.select("h9r", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"d","c"};
		String[] ConditionColumnsValues06 = {"d0","c1"};
		DBApp.select("h9r", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("h9r");
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("h9r");
	
		DBApp.select("h9r");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"g"};
		String[] ConditionColumnsValues016 = {"g4"};
		DBApp.select("h9r", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("h9r");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer019,recordSelectPointer019);
	
		DBApp.select("h9r");
	
		DBApp.select("h9r");
	
		DBApp.select("h9r");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer023,recordSelectPointer023);
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("h9r", pageSelectPointer024,recordSelectPointer024);
	
		String selectTrace0 = DBApp.getFullTrace("h9r");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 409);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 382"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ms", cols1);
		String [][] records_ms = new String[491][cols1.length];
		for(int i=0;i<491;i++)
		{
			records_ms[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ms[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ms", records_ms[i]);
		}
	
		int pageCount1 = (int)Math.ceil(491.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns10 = {"b"};
		String[] ConditionColumnsValues10 = {"b0"};
		DBApp.select("ms", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("ms");
	
		String[] ConditionColumns12 = {"d"};
		String[] ConditionColumnsValues12 = {"d3"};
		DBApp.select("ms", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"e","h","f"};
		String[] ConditionColumnsValues13 = {"e4","h0","f0"};
		DBApp.select("ms", ConditionColumns13, ConditionColumnsValues13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"o"};
		String[] ConditionColumnsValues15 = {"o1"};
		DBApp.select("ms", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"i","k","j","g","f"};
		String[] ConditionColumnsValues16 = {"i2","k7","j7","g3","f5"};
		DBApp.select("ms", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("ms");
	
		String[] ConditionColumns18 = {"o"};
		String[] ConditionColumnsValues18 = {"o2"};
		DBApp.select("ms", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"c","a","l","k","d","e"};
		String[] ConditionColumnsValues110 = {"c2","a266","l2","k2","d2","e1"};
		DBApp.select("ms", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"b","e","c"};
		String[] ConditionColumnsValues111 = {"b1","e0","c0"};
		DBApp.select("ms", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"g"};
		String[] ConditionColumnsValues113 = {"g1"};
		DBApp.select("ms", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"b","d","g","k","o","a","n"};
		String[] ConditionColumnsValues114 = {"b1","d1","g5","k1","o14","a89","n5"};
		DBApp.select("ms", ConditionColumns114, ConditionColumnsValues114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("ms");
	
		String[] ConditionColumns117 = {"k"};
		String[] ConditionColumnsValues117 = {"k9"};
		DBApp.select("ms", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("ms");
	
		String[] ConditionColumns120 = {"j","h","i","l","o","m"};
		String[] ConditionColumnsValues120 = {"j4","h0","i0","l0","o9","m1"};
		DBApp.select("ms", ConditionColumns120, ConditionColumnsValues120);
	
		DBApp.select("ms");
	
		String[] ConditionColumns122 = {"l"};
		String[] ConditionColumnsValues122 = {"l5"};
		DBApp.select("ms", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"c","j","g","o"};
		String[] ConditionColumnsValues123 = {"c0","j3","g1","o3"};
		DBApp.select("ms", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"o"};
		String[] ConditionColumnsValues124 = {"o5"};
		DBApp.select("ms", ConditionColumns124, ConditionColumnsValues124);
	
		DBApp.select("ms");
	
		int pageSelectPointer126 = genRandNum(pageCount1)-1;
		int recordSelectPointer126 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer126==pageCount1-1)
			recordSelectPointer126 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer126,recordSelectPointer126);
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((491%DBApp.dataPageSize))-1;
		DBApp.select("ms", pageSelectPointer127,recordSelectPointer127);
	
		DBApp.select("ms");
	
		String selectTrace1 = DBApp.getFullTrace("ms");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 522);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 491"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("nzx", cols0);
		String [][] records_nzx = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_nzx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nzx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nzx", records_nzx[i]);
		}
	
		int pageCount0 = (int)Math.ceil(38.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		DBApp.select("nzx");
	
		String[] ConditionColumns01 = {"i"};
		String[] ConditionColumnsValues01 = {"i1"};
		DBApp.select("nzx", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("nzx");
	
		DBApp.select("nzx");
	
		String selectTrace0 = DBApp.getFullTrace("nzx");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 44);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 38"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("f924", cols1);
		String [][] records_f924 = new String[137][cols1.length];
		for(int i=0;i<137;i++)
		{
			records_f924[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_f924[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("f924", records_f924[i]);
		}
	
		int pageCount1 = (int)Math.ceil(137.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns10 = {"a"};
		String[] ConditionColumnsValues10 = {"a37"};
		DBApp.select("f924", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((137%DBApp.dataPageSize))-1;
		DBApp.select("f924", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((137%DBApp.dataPageSize))-1;
		DBApp.select("f924", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((137%DBApp.dataPageSize))-1;
		DBApp.select("f924", pageSelectPointer13,recordSelectPointer13);
	
		String selectTrace1 = DBApp.getFullTrace("f924");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 143);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 137"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("h2", cols2);
		String [][] records_h2 = new String[263][cols2.length];
		for(int i=0;i<263;i++)
		{
			records_h2[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h2[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h2", records_h2[i]);
		}
	
		int pageCount2 = (int)Math.ceil(263.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer21,recordSelectPointer21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("h2");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer25,recordSelectPointer25);
	
		DBApp.select("h2");
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer27,recordSelectPointer27);
	
		DBApp.select("h2");
	
		String[] ConditionColumns29 = {"c"};
		String[] ConditionColumnsValues29 = {"c1"};
		DBApp.select("h2", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("h2");
	
		String[] ConditionColumns211 = {"b"};
		String[] ConditionColumnsValues211 = {"b0"};
		DBApp.select("h2", ConditionColumns211, ConditionColumnsValues211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((263%DBApp.dataPageSize))-1;
		DBApp.select("h2", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("h2");
	
		String selectTrace2 = DBApp.getFullTrace("h2");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 280);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 263"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("l5767", cols3);
		String [][] records_l5767 = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			records_l5767[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_l5767[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("l5767", records_l5767[i]);
		}
	
		int pageCount3 = (int)Math.ceil(32.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		String[] ConditionColumns30 = {"e"};
		String[] ConditionColumnsValues30 = {"e1"};
		DBApp.select("l5767", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("l5767");
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((32%DBApp.dataPageSize))-1;
		DBApp.select("l5767", pageSelectPointer32,recordSelectPointer32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((32%DBApp.dataPageSize))-1;
		DBApp.select("l5767", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((32%DBApp.dataPageSize))-1;
		DBApp.select("l5767", pageSelectPointer34,recordSelectPointer34);
	
		int pageSelectPointer35 = genRandNum(pageCount3)-1;
		int recordSelectPointer35 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer35==pageCount3-1)
			recordSelectPointer35 = genRandNum((32%DBApp.dataPageSize))-1;
		DBApp.select("l5767", pageSelectPointer35,recordSelectPointer35);
	
		DBApp.select("l5767");
	
		String[] ConditionColumns37 = {"e"};
		String[] ConditionColumnsValues37 = {"e2"};
		DBApp.select("l5767", ConditionColumns37, ConditionColumnsValues37);
	
		DBApp.select("l5767");
	
		DBApp.select("l5767");
	
		DBApp.select("l5767");
	
		String selectTrace3 = DBApp.getFullTrace("l5767");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 45);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 32"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("md2o1", cols0);
		String [][] records_md2o1 = new String[301][cols0.length];
		for(int i=0;i<301;i++)
		{
			records_md2o1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_md2o1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("md2o1", records_md2o1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(301.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("md2o1");
	
		DBApp.select("md2o1");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((301%DBApp.dataPageSize))-1;
		DBApp.select("md2o1", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"a"};
		String[] ConditionColumnsValues03 = {"a225"};
		DBApp.select("md2o1", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a274"};
		DBApp.select("md2o1", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((301%DBApp.dataPageSize))-1;
		DBApp.select("md2o1", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((301%DBApp.dataPageSize))-1;
		DBApp.select("md2o1", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("md2o1");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((301%DBApp.dataPageSize))-1;
		DBApp.select("md2o1", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a147"};
		DBApp.select("md2o1", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((301%DBApp.dataPageSize))-1;
		DBApp.select("md2o1", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"a"};
		String[] ConditionColumnsValues011 = {"a115"};
		DBApp.select("md2o1", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("md2o1");
	
		String[] ConditionColumns013 = {"a"};
		String[] ConditionColumnsValues013 = {"a55"};
		DBApp.select("md2o1", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"a"};
		String[] ConditionColumnsValues014 = {"a240"};
		DBApp.select("md2o1", ConditionColumns014, ConditionColumnsValues014);
	
		String selectTrace0 = DBApp.getFullTrace("md2o1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 318);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 301"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ips", cols1);
		String [][] records_ips = new String[473][cols1.length];
		for(int i=0;i<473;i++)
		{
			records_ips[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ips[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ips", records_ips[i]);
		}
	
		int pageCount1 = (int)Math.ceil(473.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"b","c","e"};
		String[] ConditionColumnsValues12 = {"b0","c0","e1"};
		DBApp.select("ips", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("ips");
	
		String[] ConditionColumns15 = {"d","g","e"};
		String[] ConditionColumnsValues15 = {"d3","g4","e3"};
		DBApp.select("ips", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"f","c","e"};
		String[] ConditionColumnsValues16 = {"f4","c1","e0"};
		DBApp.select("ips", ConditionColumns16, ConditionColumnsValues16);
	
		String[] ConditionColumns17 = {"e","b"};
		String[] ConditionColumnsValues17 = {"e2","b0"};
		DBApp.select("ips", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("ips");
	
		String[] ConditionColumns19 = {"g"};
		String[] ConditionColumnsValues19 = {"g0"};
		DBApp.select("ips", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("ips");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("ips");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("ips");
	
		DBApp.select("ips");
	
		String[] ConditionColumns118 = {"e","f"};
		String[] ConditionColumnsValues118 = {"e4","f1"};
		DBApp.select("ips", ConditionColumns118, ConditionColumnsValues118);
	
		DBApp.select("ips");
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((473%DBApp.dataPageSize))-1;
		DBApp.select("ips", pageSelectPointer121,recordSelectPointer121);
	
		String selectTrace1 = DBApp.getFullTrace("ips");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 497);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 473"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("kg8", cols2);
		String [][] records_kg8 = new String[337][cols2.length];
		for(int i=0;i<337;i++)
		{
			records_kg8[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_kg8[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("kg8", records_kg8[i]);
		}
	
		int pageCount2 = (int)Math.ceil(337.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		DBApp.select("kg8");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((337%DBApp.dataPageSize))-1;
		DBApp.select("kg8", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"f"};
		String[] ConditionColumnsValues22 = {"f4"};
		DBApp.select("kg8", ConditionColumns22, ConditionColumnsValues22);
	
		String[] ConditionColumns23 = {"h"};
		String[] ConditionColumnsValues23 = {"h4"};
		DBApp.select("kg8", ConditionColumns23, ConditionColumnsValues23);
	
		String selectTrace2 = DBApp.getFullTrace("kg8");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 343);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 337"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ep5b6", cols3);
		String [][] records_ep5b6 = new String[17][cols3.length];
		for(int i=0;i<17;i++)
		{
			records_ep5b6[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ep5b6[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ep5b6", records_ep5b6[i]);
		}
	
		int pageCount3 = (int)Math.ceil(17.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		String[] ConditionColumns30 = {"h"};
		String[] ConditionColumnsValues30 = {"h3"};
		DBApp.select("ep5b6", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("ep5b6");
	
		DBApp.select("ep5b6");
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((17%DBApp.dataPageSize))-1;
		DBApp.select("ep5b6", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((17%DBApp.dataPageSize))-1;
		DBApp.select("ep5b6", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"g"};
		String[] ConditionColumnsValues35 = {"g5"};
		DBApp.select("ep5b6", ConditionColumns35, ConditionColumnsValues35);
	
		String selectTrace3 = DBApp.getFullTrace("ep5b6");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 25);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 17"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("z75", cols0);
		String [][] records_z75 = new String[147][cols0.length];
		for(int i=0;i<147;i++)
		{
			records_z75[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z75[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z75", records_z75[i]);
		}
	
		int pageCount0 = (int)Math.ceil(147.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		String[] ConditionColumns09 = {"d"};
		String[] ConditionColumnsValues09 = {"d3"};
		DBApp.select("z75", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"b"};
		String[] ConditionColumnsValues013 = {"b0"};
		DBApp.select("z75", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"d"};
		String[] ConditionColumnsValues014 = {"d2"};
		DBApp.select("z75", ConditionColumns014, ConditionColumnsValues014);
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"b"};
		String[] ConditionColumnsValues016 = {"b1"};
		DBApp.select("z75", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("z75");
	
		DBApp.select("z75");
	
		String[] ConditionColumns024 = {"d"};
		String[] ConditionColumnsValues024 = {"d2"};
		DBApp.select("z75", ConditionColumns024, ConditionColumnsValues024);
	
		DBApp.select("z75");
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer026,recordSelectPointer026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer027,recordSelectPointer027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((147%DBApp.dataPageSize))-1;
		DBApp.select("z75", pageSelectPointer028,recordSelectPointer028);
	
		String selectTrace0 = DBApp.getFullTrace("z75");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 178);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 147"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rsg0", cols1);
		String [][] records_rsg0 = new String[234][cols1.length];
		for(int i=0;i<234;i++)
		{
			records_rsg0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rsg0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rsg0", records_rsg0[i]);
		}
	
		int pageCount1 = (int)Math.ceil(234.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		String[] ConditionColumns10 = {"e","f","d"};
		String[] ConditionColumnsValues10 = {"e2","f5","d1"};
		DBApp.select("rsg0", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("rsg0");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("rsg0");
	
		String[] ConditionColumns14 = {"e"};
		String[] ConditionColumnsValues14 = {"e0"};
		DBApp.select("rsg0", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"a","g"};
		String[] ConditionColumnsValues15 = {"a47","g5"};
		DBApp.select("rsg0", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"g","e"};
		String[] ConditionColumnsValues110 = {"g4","e3"};
		DBApp.select("rsg0", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"b","c","e"};
		String[] ConditionColumnsValues111 = {"b0","c1","e3"};
		DBApp.select("rsg0", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((234%DBApp.dataPageSize))-1;
		DBApp.select("rsg0", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("rsg0");
	
		DBApp.select("rsg0");
	
		String[] ConditionColumns116 = {"d"};
		String[] ConditionColumnsValues116 = {"d1"};
		DBApp.select("rsg0", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("rsg0");
	
		String selectTrace1 = DBApp.getFullTrace("rsg0");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 254);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 234"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mbq1", cols2);
		String [][] records_mbq1 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			records_mbq1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mbq1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mbq1", records_mbq1[i]);
		}
	
		int pageCount2 = (int)Math.ceil(77.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((77%DBApp.dataPageSize))-1;
		DBApp.select("mbq1", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"g"};
		String[] ConditionColumnsValues21 = {"g0"};
		DBApp.select("mbq1", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"h","b","a"};
		String[] ConditionColumnsValues22 = {"h6","b0","a22"};
		DBApp.select("mbq1", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("mbq1");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((77%DBApp.dataPageSize))-1;
		DBApp.select("mbq1", pageSelectPointer24,recordSelectPointer24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((77%DBApp.dataPageSize))-1;
		DBApp.select("mbq1", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((77%DBApp.dataPageSize))-1;
		DBApp.select("mbq1", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"b"};
		String[] ConditionColumnsValues27 = {"b1"};
		DBApp.select("mbq1", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"c","b","h","g"};
		String[] ConditionColumnsValues28 = {"c2","b1","h3","g3"};
		DBApp.select("mbq1", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"e"};
		String[] ConditionColumnsValues29 = {"e2"};
		DBApp.select("mbq1", ConditionColumns29, ConditionColumnsValues29);
	
		String[] ConditionColumns210 = {"c"};
		String[] ConditionColumnsValues210 = {"c0"};
		DBApp.select("mbq1", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"b"};
		String[] ConditionColumnsValues211 = {"b1"};
		DBApp.select("mbq1", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"e"};
		String[] ConditionColumnsValues212 = {"e3"};
		DBApp.select("mbq1", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("mbq1");
	
		String[] ConditionColumns214 = {"f"};
		String[] ConditionColumnsValues214 = {"f2"};
		DBApp.select("mbq1", ConditionColumns214, ConditionColumnsValues214);
	
		DBApp.select("mbq1");
	
		String[] ConditionColumns216 = {"f"};
		String[] ConditionColumnsValues216 = {"f1"};
		DBApp.select("mbq1", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"h"};
		String[] ConditionColumnsValues217 = {"h7"};
		DBApp.select("mbq1", ConditionColumns217, ConditionColumnsValues217);
	
		int pageSelectPointer218 = genRandNum(pageCount2)-1;
		int recordSelectPointer218 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer218==pageCount2-1)
			recordSelectPointer218 = genRandNum((77%DBApp.dataPageSize))-1;
		DBApp.select("mbq1", pageSelectPointer218,recordSelectPointer218);
	
		DBApp.select("mbq1");
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((77%DBApp.dataPageSize))-1;
		DBApp.select("mbq1", pageSelectPointer220,recordSelectPointer220);
	
		String[] ConditionColumns221 = {"b","d","c"};
		String[] ConditionColumnsValues221 = {"b1","d3","c1"};
		DBApp.select("mbq1", ConditionColumns221, ConditionColumnsValues221);
	
		String[] ConditionColumns222 = {"g"};
		String[] ConditionColumnsValues222 = {"g1"};
		DBApp.select("mbq1", ConditionColumns222, ConditionColumnsValues222);
	
		String selectTrace2 = DBApp.getFullTrace("mbq1");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 102);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 77"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ma", cols0);
		String [][] records_ma = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			records_ma[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ma[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ma", records_ma[i]);
		}
	
		int pageCount0 = (int)Math.ceil(19.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("ma", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("ma");
	
		DBApp.select("ma");
	
		String[] ConditionColumns03 = {"f"};
		String[] ConditionColumnsValues03 = {"f0"};
		DBApp.select("ma", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"g","d","c"};
		String[] ConditionColumnsValues04 = {"g6","d1","c1"};
		DBApp.select("ma", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"h","e"};
		String[] ConditionColumnsValues05 = {"h6","e1"};
		DBApp.select("ma", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("ma", pageSelectPointer06,recordSelectPointer06);
	
		String[] ConditionColumns07 = {"b"};
		String[] ConditionColumnsValues07 = {"b1"};
		DBApp.select("ma", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"d"};
		String[] ConditionColumnsValues08 = {"d3"};
		DBApp.select("ma", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("ma");
	
		String[] ConditionColumns010 = {"d","g"};
		String[] ConditionColumnsValues010 = {"d0","g1"};
		DBApp.select("ma", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("ma");
	
		String[] ConditionColumns012 = {"b"};
		String[] ConditionColumnsValues012 = {"b0"};
		DBApp.select("ma", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("ma");
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("ma", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"a"};
		String[] ConditionColumnsValues015 = {"a8"};
		DBApp.select("ma", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"b"};
		String[] ConditionColumnsValues016 = {"b0"};
		DBApp.select("ma", ConditionColumns016, ConditionColumnsValues016);
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("ma", pageSelectPointer017,recordSelectPointer017);
	
		DBApp.select("ma");
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("ma", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"b"};
		String[] ConditionColumnsValues020 = {"b0"};
		DBApp.select("ma", ConditionColumns020, ConditionColumnsValues020);
	
		String[] ConditionColumns021 = {"d"};
		String[] ConditionColumnsValues021 = {"d2"};
		DBApp.select("ma", ConditionColumns021, ConditionColumnsValues021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((19%DBApp.dataPageSize))-1;
		DBApp.select("ma", pageSelectPointer022,recordSelectPointer022);
	
		String selectTrace0 = DBApp.getFullTrace("ma");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 44);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 19"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("cz8", cols1);
		String [][] records_cz8 = new String[424][cols1.length];
		for(int i=0;i<424;i++)
		{
			records_cz8[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cz8[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cz8", records_cz8[i]);
		}
	
		int pageCount1 = (int)Math.ceil(424.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("cz8");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("cz8");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b1"};
		DBApp.select("cz8", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("cz8");
	
		String[] ConditionColumns16 = {"b"};
		String[] ConditionColumnsValues16 = {"b1"};
		DBApp.select("cz8", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("cz8");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("cz8");
	
		DBApp.select("cz8");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer112,recordSelectPointer112);
	
		String[] ConditionColumns113 = {"a"};
		String[] ConditionColumnsValues113 = {"a33"};
		DBApp.select("cz8", ConditionColumns113, ConditionColumnsValues113);
	
		DBApp.select("cz8");
	
		DBApp.select("cz8");
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer116,recordSelectPointer116);
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b0"};
		DBApp.select("cz8", ConditionColumns117, ConditionColumnsValues117);
	
		String[] ConditionColumns118 = {"a"};
		String[] ConditionColumnsValues118 = {"a163"};
		DBApp.select("cz8", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"a"};
		String[] ConditionColumnsValues119 = {"a322"};
		DBApp.select("cz8", ConditionColumns119, ConditionColumnsValues119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer120,recordSelectPointer120);
	
		String[] ConditionColumns121 = {"b"};
		String[] ConditionColumnsValues121 = {"b1"};
		DBApp.select("cz8", ConditionColumns121, ConditionColumnsValues121);
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((424%DBApp.dataPageSize))-1;
		DBApp.select("cz8", pageSelectPointer122,recordSelectPointer122);
	
		String[] ConditionColumns123 = {"b"};
		String[] ConditionColumnsValues123 = {"b0"};
		DBApp.select("cz8", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"b"};
		String[] ConditionColumnsValues124 = {"b1"};
		DBApp.select("cz8", ConditionColumns124, ConditionColumnsValues124);
	
		DBApp.select("cz8");
	
		DBApp.select("cz8");
	
		DBApp.select("cz8");
	
		DBApp.select("cz8");
	
		String selectTrace1 = DBApp.getFullTrace("cz8");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 455);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 424"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("vt7", cols0);
		String [][] records_vt7 = new String[271][cols0.length];
		for(int i=0;i<271;i++)
		{
			records_vt7[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_vt7[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("vt7", records_vt7[i]);
		}
	
		int pageCount0 = (int)Math.ceil(271.0/DBApp.dataPageSize);
	
		 //performing 24 selects:
	
		DBApp.select("vt7");
	
		String[] ConditionColumns01 = {"i"};
		String[] ConditionColumnsValues01 = {"i0"};
		DBApp.select("vt7", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((271%DBApp.dataPageSize))-1;
		DBApp.select("vt7", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("vt7");
	
		String[] ConditionColumns04 = {"e"};
		String[] ConditionColumnsValues04 = {"e1"};
		DBApp.select("vt7", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("vt7");
	
		DBApp.select("vt7");
	
		String[] ConditionColumns07 = {"i","f"};
		String[] ConditionColumnsValues07 = {"i4","f4"};
		DBApp.select("vt7", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("vt7");
	
		String[] ConditionColumns09 = {"h","e","j","c","d"};
		String[] ConditionColumnsValues09 = {"h6","e0","j0","c2","d2"};
		DBApp.select("vt7", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("vt7");
	
		String[] ConditionColumns011 = {"g","b","h","c","i"};
		String[] ConditionColumnsValues011 = {"g3","b1","h7","c0","i3"};
		DBApp.select("vt7", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"d","h"};
		String[] ConditionColumnsValues012 = {"d1","h5"};
		DBApp.select("vt7", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"h","d"};
		String[] ConditionColumnsValues013 = {"h7","d3"};
		DBApp.select("vt7", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"i"};
		String[] ConditionColumnsValues014 = {"i5"};
		DBApp.select("vt7", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("vt7");
	
		DBApp.select("vt7");
	
		DBApp.select("vt7");
	
		String[] ConditionColumns018 = {"j"};
		String[] ConditionColumnsValues018 = {"j0"};
		DBApp.select("vt7", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"f"};
		String[] ConditionColumnsValues019 = {"f0"};
		DBApp.select("vt7", ConditionColumns019, ConditionColumnsValues019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((271%DBApp.dataPageSize))-1;
		DBApp.select("vt7", pageSelectPointer020,recordSelectPointer020);
	
		String[] ConditionColumns021 = {"b"};
		String[] ConditionColumnsValues021 = {"b0"};
		DBApp.select("vt7", ConditionColumns021, ConditionColumnsValues021);
	
		DBApp.select("vt7");
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((271%DBApp.dataPageSize))-1;
		DBApp.select("vt7", pageSelectPointer023,recordSelectPointer023);
	
		String selectTrace0 = DBApp.getFullTrace("vt7");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 297);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 271"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a5v36", cols0);
		String [][] records_a5v36 = new String[367][cols0.length];
		for(int i=0;i<367;i++)
		{
			records_a5v36[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a5v36[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a5v36", records_a5v36[i]);
		}
	
		int pageCount0 = (int)Math.ceil(367.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns00 = {"b"};
		String[] ConditionColumnsValues00 = {"b1"};
		DBApp.select("a5v36", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"f"};
		String[] ConditionColumnsValues01 = {"f0"};
		DBApp.select("a5v36", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("a5v36");
	
		String[] ConditionColumns03 = {"f"};
		String[] ConditionColumnsValues03 = {"f4"};
		DBApp.select("a5v36", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"f"};
		String[] ConditionColumnsValues04 = {"f5"};
		DBApp.select("a5v36", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"f"};
		String[] ConditionColumnsValues05 = {"f5"};
		DBApp.select("a5v36", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((367%DBApp.dataPageSize))-1;
		DBApp.select("a5v36", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((367%DBApp.dataPageSize))-1;
		DBApp.select("a5v36", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((367%DBApp.dataPageSize))-1;
		DBApp.select("a5v36", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"e"};
		String[] ConditionColumnsValues09 = {"e2"};
		DBApp.select("a5v36", ConditionColumns09, ConditionColumnsValues09);
	
		String[] ConditionColumns010 = {"e"};
		String[] ConditionColumnsValues010 = {"e2"};
		DBApp.select("a5v36", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("a5v36");
	
		DBApp.select("a5v36");
	
		String[] ConditionColumns013 = {"g"};
		String[] ConditionColumnsValues013 = {"g6"};
		DBApp.select("a5v36", ConditionColumns013, ConditionColumnsValues013);
	
		String selectTrace0 = DBApp.getFullTrace("a5v36");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 383);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 367"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("nnsx", cols0);
		String [][] records_nnsx = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			records_nnsx[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_nnsx[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("nnsx", records_nnsx[i]);
		}
	
		int pageCount0 = (int)Math.ceil(33.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((33%DBApp.dataPageSize))-1;
		DBApp.select("nnsx", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((33%DBApp.dataPageSize))-1;
		DBApp.select("nnsx", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"b","e"};
		String[] ConditionColumnsValues02 = {"b0","e1"};
		DBApp.select("nnsx", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((33%DBApp.dataPageSize))-1;
		DBApp.select("nnsx", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("nnsx");
	
		DBApp.select("nnsx");
	
		DBApp.select("nnsx");
	
		String selectTrace0 = DBApp.getFullTrace("nnsx");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 42);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 33"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("no4", cols0);
		String [][] records_no4 = new String[274][cols0.length];
		for(int i=0;i<274;i++)
		{
			records_no4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_no4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("no4", records_no4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(274.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		DBApp.select("no4");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((274%DBApp.dataPageSize))-1;
		DBApp.select("no4", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("no4");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((274%DBApp.dataPageSize))-1;
		DBApp.select("no4", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((274%DBApp.dataPageSize))-1;
		DBApp.select("no4", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"e"};
		String[] ConditionColumnsValues05 = {"e3"};
		DBApp.select("no4", ConditionColumns05, ConditionColumnsValues05);
	
		String selectTrace0 = DBApp.getFullTrace("no4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 282);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 274"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("c44", cols1);
		String [][] records_c44 = new String[207][cols1.length];
		for(int i=0;i<207;i++)
		{
			records_c44[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c44[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c44", records_c44[i]);
		}
	
		int pageCount1 = (int)Math.ceil(207.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("c44");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"a"};
		String[] ConditionColumnsValues13 = {"a114"};
		DBApp.select("c44", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("c44");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("c44");
	
		String[] ConditionColumns18 = {"a"};
		String[] ConditionColumnsValues18 = {"a36"};
		DBApp.select("c44", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("c44");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer111,recordSelectPointer111);
	
		String[] ConditionColumns112 = {"a"};
		String[] ConditionColumnsValues112 = {"a51"};
		DBApp.select("c44", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("c44");
	
		DBApp.select("c44");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("c44", pageSelectPointer115,recordSelectPointer115);
	
		String selectTrace1 = DBApp.getFullTrace("c44");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 225);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 207"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("j0989", cols2);
		String [][] records_j0989 = new String[394][cols2.length];
		for(int i=0;i<394;i++)
		{
			records_j0989[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j0989[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j0989", records_j0989[i]);
		}
	
		int pageCount2 = (int)Math.ceil(394.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("j0989");
	
		String[] ConditionColumns21 = {"b"};
		String[] ConditionColumnsValues21 = {"b1"};
		DBApp.select("j0989", ConditionColumns21, ConditionColumnsValues21);
	
		DBApp.select("j0989");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((394%DBApp.dataPageSize))-1;
		DBApp.select("j0989", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((394%DBApp.dataPageSize))-1;
		DBApp.select("j0989", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"a"};
		String[] ConditionColumnsValues25 = {"a304"};
		DBApp.select("j0989", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((394%DBApp.dataPageSize))-1;
		DBApp.select("j0989", pageSelectPointer26,recordSelectPointer26);
	
		String[] ConditionColumns27 = {"b"};
		String[] ConditionColumnsValues27 = {"b1"};
		DBApp.select("j0989", ConditionColumns27, ConditionColumnsValues27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((394%DBApp.dataPageSize))-1;
		DBApp.select("j0989", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("j0989");
	
		String[] ConditionColumns210 = {"b"};
		String[] ConditionColumnsValues210 = {"b0"};
		DBApp.select("j0989", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"a"};
		String[] ConditionColumnsValues211 = {"a98"};
		DBApp.select("j0989", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"b"};
		String[] ConditionColumnsValues212 = {"b1"};
		DBApp.select("j0989", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("j0989");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((394%DBApp.dataPageSize))-1;
		DBApp.select("j0989", pageSelectPointer214,recordSelectPointer214);
	
		String selectTrace2 = DBApp.getFullTrace("j0989");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 411);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 394"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("k1d0p", cols3);
		String [][] records_k1d0p = new String[386][cols3.length];
		for(int i=0;i<386;i++)
		{
			records_k1d0p[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_k1d0p[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("k1d0p", records_k1d0p[i]);
		}
	
		int pageCount3 = (int)Math.ceil(386.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns30 = {"j","h","d"};
		String[] ConditionColumnsValues30 = {"j7","h1","d1"};
		DBApp.select("k1d0p", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("k1d0p");
	
		int pageSelectPointer32 = genRandNum(pageCount3)-1;
		int recordSelectPointer32 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer32==pageCount3-1)
			recordSelectPointer32 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("k1d0p", pageSelectPointer32,recordSelectPointer32);
	
		DBApp.select("k1d0p");
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("k1d0p", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"a"};
		String[] ConditionColumnsValues35 = {"a174"};
		DBApp.select("k1d0p", ConditionColumns35, ConditionColumnsValues35);
	
		DBApp.select("k1d0p");
	
		DBApp.select("k1d0p");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("k1d0p", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("k1d0p");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("k1d0p", pageSelectPointer310,recordSelectPointer310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("k1d0p", pageSelectPointer311,recordSelectPointer311);
	
		String[] ConditionColumns312 = {"h","b"};
		String[] ConditionColumnsValues312 = {"h0","b0"};
		DBApp.select("k1d0p", ConditionColumns312, ConditionColumnsValues312);
	
		String[] ConditionColumns313 = {"c","j","e"};
		String[] ConditionColumnsValues313 = {"c1","j1","e1"};
		DBApp.select("k1d0p", ConditionColumns313, ConditionColumnsValues313);
	
		DBApp.select("k1d0p");
	
		DBApp.select("k1d0p");
	
		DBApp.select("k1d0p");
	
		String[] ConditionColumns317 = {"d","j","b"};
		String[] ConditionColumnsValues317 = {"d3","j1","b1"};
		DBApp.select("k1d0p", ConditionColumns317, ConditionColumnsValues317);
	
		DBApp.select("k1d0p");
	
		String[] ConditionColumns319 = {"a","j","h","b"};
		String[] ConditionColumnsValues319 = {"a254","j4","h6","b0"};
		DBApp.select("k1d0p", ConditionColumns319, ConditionColumnsValues319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((386%DBApp.dataPageSize))-1;
		DBApp.select("k1d0p", pageSelectPointer320,recordSelectPointer320);
	
		String[] ConditionColumns321 = {"h"};
		String[] ConditionColumnsValues321 = {"h6"};
		DBApp.select("k1d0p", ConditionColumns321, ConditionColumnsValues321);
	
		DBApp.select("k1d0p");
	
		String selectTrace3 = DBApp.getFullTrace("k1d0p");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 411);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 386"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e66", cols4);
		String [][] records_e66 = new String[486][cols4.length];
		for(int i=0;i<486;i++)
		{
			records_e66[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_e66[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("e66", records_e66[i]);
		}
	
		int pageCount4 = (int)Math.ceil(486.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		String[] ConditionColumns40 = {"g","b","h","e"};
		String[] ConditionColumnsValues40 = {"g3","b1","h3","e4"};
		DBApp.select("e66", ConditionColumns40, ConditionColumnsValues40);
	
		String[] ConditionColumns41 = {"c","i","e","h"};
		String[] ConditionColumnsValues41 = {"c0","i3","e1","h6"};
		DBApp.select("e66", ConditionColumns41, ConditionColumnsValues41);
	
		DBApp.select("e66");
	
		String[] ConditionColumns43 = {"c","h"};
		String[] ConditionColumnsValues43 = {"c1","h6"};
		DBApp.select("e66", ConditionColumns43, ConditionColumnsValues43);
	
		DBApp.select("e66");
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((486%DBApp.dataPageSize))-1;
		DBApp.select("e66", pageSelectPointer45,recordSelectPointer45);
	
		DBApp.select("e66");
	
		String[] ConditionColumns47 = {"f"};
		String[] ConditionColumnsValues47 = {"f2"};
		DBApp.select("e66", ConditionColumns47, ConditionColumnsValues47);
	
		DBApp.select("e66");
	
		String[] ConditionColumns49 = {"d","c","h"};
		String[] ConditionColumnsValues49 = {"d0","c1","h0"};
		DBApp.select("e66", ConditionColumns49, ConditionColumnsValues49);
	
		String[] ConditionColumns410 = {"h"};
		String[] ConditionColumnsValues410 = {"h0"};
		DBApp.select("e66", ConditionColumns410, ConditionColumnsValues410);
	
		String[] ConditionColumns411 = {"c","h","b"};
		String[] ConditionColumnsValues411 = {"c0","h2","b0"};
		DBApp.select("e66", ConditionColumns411, ConditionColumnsValues411);
	
		String[] ConditionColumns412 = {"e"};
		String[] ConditionColumnsValues412 = {"e0"};
		DBApp.select("e66", ConditionColumns412, ConditionColumnsValues412);
	
		int pageSelectPointer413 = genRandNum(pageCount4)-1;
		int recordSelectPointer413 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer413==pageCount4-1)
			recordSelectPointer413 = genRandNum((486%DBApp.dataPageSize))-1;
		DBApp.select("e66", pageSelectPointer413,recordSelectPointer413);
	
		int pageSelectPointer414 = genRandNum(pageCount4)-1;
		int recordSelectPointer414 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer414==pageCount4-1)
			recordSelectPointer414 = genRandNum((486%DBApp.dataPageSize))-1;
		DBApp.select("e66", pageSelectPointer414,recordSelectPointer414);
	
		String selectTrace4 = DBApp.getFullTrace("e66");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 503);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 486"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ehz", cols0);
		String [][] records_ehz = new String[457][cols0.length];
		for(int i=0;i<457;i++)
		{
			records_ehz[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ehz[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ehz", records_ehz[i]);
		}
	
		int pageCount0 = (int)Math.ceil(457.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns00 = {"f","d","b"};
		String[] ConditionColumnsValues00 = {"f0","d2","b0"};
		DBApp.select("ehz", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e0"};
		DBApp.select("ehz", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"g"};
		String[] ConditionColumnsValues02 = {"g5"};
		DBApp.select("ehz", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("ehz");
	
		String[] ConditionColumns05 = {"f"};
		String[] ConditionColumnsValues05 = {"f4"};
		DBApp.select("ehz", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"g"};
		String[] ConditionColumnsValues06 = {"g5"};
		DBApp.select("ehz", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("ehz");
	
		String[] ConditionColumns08 = {"b"};
		String[] ConditionColumnsValues08 = {"b0"};
		DBApp.select("ehz", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"f"};
		String[] ConditionColumnsValues011 = {"f3"};
		DBApp.select("ehz", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"c","g"};
		String[] ConditionColumnsValues012 = {"c0","g4"};
		DBApp.select("ehz", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("ehz");
	
		DBApp.select("ehz");
	
		DBApp.select("ehz");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"c"};
		String[] ConditionColumnsValues018 = {"c1"};
		DBApp.select("ehz", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"f"};
		String[] ConditionColumnsValues020 = {"f0"};
		DBApp.select("ehz", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("ehz");
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer022,recordSelectPointer022);
	
		DBApp.select("ehz");
	
		String[] ConditionColumns024 = {"e","f","a"};
		String[] ConditionColumnsValues024 = {"e3","f4","a298"};
		DBApp.select("ehz", ConditionColumns024, ConditionColumnsValues024);
	
		String[] ConditionColumns025 = {"c","e","h"};
		String[] ConditionColumnsValues025 = {"c0","e3","h7"};
		DBApp.select("ehz", ConditionColumns025, ConditionColumnsValues025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer026,recordSelectPointer026);
	
		int pageSelectPointer027 = genRandNum(pageCount0)-1;
		int recordSelectPointer027 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer027==pageCount0-1)
			recordSelectPointer027 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ehz", pageSelectPointer027,recordSelectPointer027);
	
		String selectTrace0 = DBApp.getFullTrace("ehz");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 487);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 457"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("y8gzs", cols1);
		String [][] records_y8gzs = new String[319][cols1.length];
		for(int i=0;i<319;i++)
		{
			records_y8gzs[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y8gzs[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y8gzs", records_y8gzs[i]);
		}
	
		int pageCount1 = (int)Math.ceil(319.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		DBApp.select("y8gzs");
	
		DBApp.select("y8gzs");
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("y8gzs", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("y8gzs");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("y8gzs", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("y8gzs");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((319%DBApp.dataPageSize))-1;
		DBApp.select("y8gzs", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"d"};
		String[] ConditionColumnsValues17 = {"d1"};
		DBApp.select("y8gzs", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("y8gzs");
	
		DBApp.select("y8gzs");
	
		String selectTrace1 = DBApp.getFullTrace("y8gzs");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 331);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 319"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("p9zr4", cols0);
		String [][] records_p9zr4 = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			records_p9zr4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p9zr4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p9zr4", records_p9zr4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(59.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((59%DBApp.dataPageSize))-1;
		DBApp.select("p9zr4", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("p9zr4");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((59%DBApp.dataPageSize))-1;
		DBApp.select("p9zr4", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("p9zr4");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((59%DBApp.dataPageSize))-1;
		DBApp.select("p9zr4", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("p9zr4");
	
		DBApp.select("p9zr4");
	
		String[] ConditionColumns07 = {"d"};
		String[] ConditionColumnsValues07 = {"d3"};
		DBApp.select("p9zr4", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"g"};
		String[] ConditionColumnsValues08 = {"g0"};
		DBApp.select("p9zr4", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"i"};
		String[] ConditionColumnsValues09 = {"i7"};
		DBApp.select("p9zr4", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((59%DBApp.dataPageSize))-1;
		DBApp.select("p9zr4", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((59%DBApp.dataPageSize))-1;
		DBApp.select("p9zr4", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"d","c"};
		String[] ConditionColumnsValues012 = {"d2","c2"};
		DBApp.select("p9zr4", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"b","g","f"};
		String[] ConditionColumnsValues013 = {"b1","g6","f5"};
		DBApp.select("p9zr4", ConditionColumns013, ConditionColumnsValues013);
	
		String[] ConditionColumns014 = {"b"};
		String[] ConditionColumnsValues014 = {"b1"};
		DBApp.select("p9zr4", ConditionColumns014, ConditionColumnsValues014);
	
		String selectTrace0 = DBApp.getFullTrace("p9zr4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 76);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 59"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("mv1y", cols0);
		String [][] records_mv1y = new String[172][cols0.length];
		for(int i=0;i<172;i++)
		{
			records_mv1y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mv1y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mv1y", records_mv1y[i]);
		}
	
		int pageCount0 = (int)Math.ceil(172.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns00 = {"e"};
		String[] ConditionColumnsValues00 = {"e4"};
		DBApp.select("mv1y", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("mv1y");
	
		String[] ConditionColumns02 = {"h"};
		String[] ConditionColumnsValues02 = {"h3"};
		DBApp.select("mv1y", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"a"};
		String[] ConditionColumnsValues06 = {"a45"};
		DBApp.select("mv1y", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("mv1y");
	
		String[] ConditionColumns08 = {"b"};
		String[] ConditionColumnsValues08 = {"b1"};
		DBApp.select("mv1y", ConditionColumns08, ConditionColumnsValues08);
	
		DBApp.select("mv1y");
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("mv1y");
	
		String[] ConditionColumns014 = {"d"};
		String[] ConditionColumnsValues014 = {"d0"};
		DBApp.select("mv1y", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"d","h"};
		String[] ConditionColumnsValues015 = {"d0","h4"};
		DBApp.select("mv1y", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"b"};
		String[] ConditionColumnsValues017 = {"b1"};
		DBApp.select("mv1y", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"b"};
		String[] ConditionColumnsValues018 = {"b1"};
		DBApp.select("mv1y", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"e"};
		String[] ConditionColumnsValues019 = {"e1"};
		DBApp.select("mv1y", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"a"};
		String[] ConditionColumnsValues020 = {"a113"};
		DBApp.select("mv1y", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((172%DBApp.dataPageSize))-1;
		DBApp.select("mv1y", pageSelectPointer021,recordSelectPointer021);
	
		DBApp.select("mv1y");
	
		DBApp.select("mv1y");
	
		DBApp.select("mv1y");
	
		String[] ConditionColumns025 = {"g"};
		String[] ConditionColumnsValues025 = {"g0"};
		DBApp.select("mv1y", ConditionColumns025, ConditionColumnsValues025);
	
		String[] ConditionColumns026 = {"b"};
		String[] ConditionColumnsValues026 = {"b1"};
		DBApp.select("mv1y", ConditionColumns026, ConditionColumnsValues026);
	
		DBApp.select("mv1y");
	
		String[] ConditionColumns028 = {"g"};
		String[] ConditionColumnsValues028 = {"g6"};
		DBApp.select("mv1y", ConditionColumns028, ConditionColumnsValues028);
	
		String selectTrace0 = DBApp.getFullTrace("mv1y");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 203);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 172"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("ayd5", cols1);
		String [][] records_ayd5 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			records_ayd5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ayd5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ayd5", records_ayd5[i]);
		}
	
		int pageCount1 = (int)Math.ceil(45.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		String[] ConditionColumns10 = {"e"};
		String[] ConditionColumnsValues10 = {"e2"};
		DBApp.select("ayd5", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"c"};
		String[] ConditionColumnsValues11 = {"c0"};
		DBApp.select("ayd5", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b0"};
		DBApp.select("ayd5", ConditionColumns14, ConditionColumnsValues14);
	
		String[] ConditionColumns15 = {"e"};
		String[] ConditionColumnsValues15 = {"e4"};
		DBApp.select("ayd5", ConditionColumns15, ConditionColumnsValues15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("ayd5");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer18,recordSelectPointer18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"b"};
		String[] ConditionColumnsValues110 = {"b1"};
		DBApp.select("ayd5", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"d"};
		String[] ConditionColumnsValues111 = {"d2"};
		DBApp.select("ayd5", ConditionColumns111, ConditionColumnsValues111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("ayd5");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer114,recordSelectPointer114);
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((45%DBApp.dataPageSize))-1;
		DBApp.select("ayd5", pageSelectPointer115,recordSelectPointer115);
	
		DBApp.select("ayd5");
	
		String[] ConditionColumns117 = {"e"};
		String[] ConditionColumnsValues117 = {"e2"};
		DBApp.select("ayd5", ConditionColumns117, ConditionColumnsValues117);
	
		String selectTrace1 = DBApp.getFullTrace("ayd5");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 65);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 45"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("av4l1", cols0);
		String [][] records_av4l1 = new String[156][cols0.length];
		for(int i=0;i<156;i++)
		{
			records_av4l1[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_av4l1[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("av4l1", records_av4l1[i]);
		}
	
		int pageCount0 = (int)Math.ceil(156.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((156%DBApp.dataPageSize))-1;
		DBApp.select("av4l1", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e4"};
		DBApp.select("av4l1", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((156%DBApp.dataPageSize))-1;
		DBApp.select("av4l1", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"d","a"};
		String[] ConditionColumnsValues03 = {"d1","a1"};
		DBApp.select("av4l1", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("av4l1");
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c0"};
		DBApp.select("av4l1", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((156%DBApp.dataPageSize))-1;
		DBApp.select("av4l1", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("av4l1");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((156%DBApp.dataPageSize))-1;
		DBApp.select("av4l1", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((156%DBApp.dataPageSize))-1;
		DBApp.select("av4l1", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"b"};
		String[] ConditionColumnsValues010 = {"b1"};
		DBApp.select("av4l1", ConditionColumns010, ConditionColumnsValues010);
	
		DBApp.select("av4l1");
	
		String selectTrace0 = DBApp.getFullTrace("av4l1");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 170);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 156"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("g12", cols1);
		String [][] records_g12 = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			records_g12[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g12[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g12", records_g12[i]);
		}
	
		int pageCount1 = (int)Math.ceil(22.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((22%DBApp.dataPageSize))-1;
		DBApp.select("g12", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("g12");
	
		String selectTrace1 = DBApp.getFullTrace("g12");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 26);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 22"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("d4sx8", cols0);
		String [][] records_d4sx8 = new String[327][cols0.length];
		for(int i=0;i<327;i++)
		{
			records_d4sx8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d4sx8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d4sx8", records_d4sx8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(327.0/DBApp.dataPageSize);
	
		 //performing 7 selects:
	
		DBApp.select("d4sx8");
	
		String[] ConditionColumns01 = {"j"};
		String[] ConditionColumnsValues01 = {"j4"};
		DBApp.select("d4sx8", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"c","k","m","f"};
		String[] ConditionColumnsValues02 = {"c1","k8","m4","f4"};
		DBApp.select("d4sx8", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("d4sx8");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((327%DBApp.dataPageSize))-1;
		DBApp.select("d4sx8", pageSelectPointer04,recordSelectPointer04);
	
		DBApp.select("d4sx8");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((327%DBApp.dataPageSize))-1;
		DBApp.select("d4sx8", pageSelectPointer06,recordSelectPointer06);
	
		String selectTrace0 = DBApp.getFullTrace("d4sx8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 336);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 327"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p48e", cols1);
		String [][] records_p48e = new String[270][cols1.length];
		for(int i=0;i<270;i++)
		{
			records_p48e[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_p48e[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("p48e", records_p48e[i]);
		}
	
		int pageCount1 = (int)Math.ceil(270.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("p48e");
	
		String[] ConditionColumns11 = {"a"};
		String[] ConditionColumnsValues11 = {"a235"};
		DBApp.select("p48e", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c0"};
		DBApp.select("p48e", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("p48e");
	
		DBApp.select("p48e");
	
		DBApp.select("p48e");
	
		String[] ConditionColumns18 = {"f"};
		String[] ConditionColumnsValues18 = {"f2"};
		DBApp.select("p48e", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("p48e");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer113,recordSelectPointer113);
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((270%DBApp.dataPageSize))-1;
		DBApp.select("p48e", pageSelectPointer114,recordSelectPointer114);
	
		String[] ConditionColumns115 = {"a"};
		String[] ConditionColumnsValues115 = {"a188"};
		DBApp.select("p48e", ConditionColumns115, ConditionColumnsValues115);
	
		String selectTrace1 = DBApp.getFullTrace("p48e");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 288);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 270"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m86", cols2);
		String [][] records_m86 = new String[229][cols2.length];
		for(int i=0;i<229;i++)
		{
			records_m86[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m86[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m86", records_m86[i]);
		}
	
		int pageCount2 = (int)Math.ceil(229.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((229%DBApp.dataPageSize))-1;
		DBApp.select("m86", pageSelectPointer20,recordSelectPointer20);
	
		String[] ConditionColumns21 = {"h"};
		String[] ConditionColumnsValues21 = {"h4"};
		DBApp.select("m86", ConditionColumns21, ConditionColumnsValues21);
	
		String[] ConditionColumns22 = {"l","e","b","g","d"};
		String[] ConditionColumnsValues22 = {"l1","e0","b1","g5","d1"};
		DBApp.select("m86", ConditionColumns22, ConditionColumnsValues22);
	
		String selectTrace2 = DBApp.getFullTrace("m86");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 234);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 229"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("ksp76", cols3);
		String [][] records_ksp76 = new String[166][cols3.length];
		for(int i=0;i<166;i++)
		{
			records_ksp76[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ksp76[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ksp76", records_ksp76[i]);
		}
	
		int pageCount3 = (int)Math.ceil(166.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("ksp76");
	
		DBApp.select("ksp76");
	
		String[] ConditionColumns32 = {"d"};
		String[] ConditionColumnsValues32 = {"d0"};
		DBApp.select("ksp76", ConditionColumns32, ConditionColumnsValues32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((166%DBApp.dataPageSize))-1;
		DBApp.select("ksp76", pageSelectPointer33,recordSelectPointer33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((166%DBApp.dataPageSize))-1;
		DBApp.select("ksp76", pageSelectPointer34,recordSelectPointer34);
	
		String[] ConditionColumns35 = {"c"};
		String[] ConditionColumnsValues35 = {"c0"};
		DBApp.select("ksp76", ConditionColumns35, ConditionColumnsValues35);
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((166%DBApp.dataPageSize))-1;
		DBApp.select("ksp76", pageSelectPointer36,recordSelectPointer36);
	
		String[] ConditionColumns37 = {"c"};
		String[] ConditionColumnsValues37 = {"c0"};
		DBApp.select("ksp76", ConditionColumns37, ConditionColumnsValues37);
	
		String[] ConditionColumns38 = {"d"};
		String[] ConditionColumnsValues38 = {"d2"};
		DBApp.select("ksp76", ConditionColumns38, ConditionColumnsValues38);
	
		String selectTrace3 = DBApp.getFullTrace("ksp76");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 177);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 166"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("i8", cols0);
		String [][] records_i8 = new String[373][cols0.length];
		for(int i=0;i<373;i++)
		{
			records_i8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i8", records_i8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(373.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("i8", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("i8", pageSelectPointer01,recordSelectPointer01);
	
		String[] ConditionColumns02 = {"b"};
		String[] ConditionColumnsValues02 = {"b1"};
		DBApp.select("i8", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("i8");
	
		String[] ConditionColumns04 = {"f"};
		String[] ConditionColumnsValues04 = {"f2"};
		DBApp.select("i8", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"e","g","b"};
		String[] ConditionColumnsValues05 = {"e4","g0","b0"};
		DBApp.select("i8", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"c"};
		String[] ConditionColumnsValues06 = {"c0"};
		DBApp.select("i8", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"g","e"};
		String[] ConditionColumnsValues07 = {"g2","e2"};
		DBApp.select("i8", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("i8", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"e"};
		String[] ConditionColumnsValues09 = {"e2"};
		DBApp.select("i8", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((373%DBApp.dataPageSize))-1;
		DBApp.select("i8", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"e","d"};
		String[] ConditionColumnsValues011 = {"e1","d1"};
		DBApp.select("i8", ConditionColumns011, ConditionColumnsValues011);
	
		String[] ConditionColumns012 = {"h","a"};
		String[] ConditionColumnsValues012 = {"h2","a258"};
		DBApp.select("i8", ConditionColumns012, ConditionColumnsValues012);
	
		String selectTrace0 = DBApp.getFullTrace("i8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 388);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 373"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("ta", cols1);
		String [][] records_ta = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			records_ta[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ta[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ta", records_ta[i]);
		}
	
		int pageCount1 = (int)Math.ceil(34.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		DBApp.select("ta");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((34%DBApp.dataPageSize))-1;
		DBApp.select("ta", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"b"};
		String[] ConditionColumnsValues12 = {"b0"};
		DBApp.select("ta", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("ta");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((34%DBApp.dataPageSize))-1;
		DBApp.select("ta", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("ta");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((34%DBApp.dataPageSize))-1;
		DBApp.select("ta", pageSelectPointer16,recordSelectPointer16);
	
		DBApp.select("ta");
	
		DBApp.select("ta");
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((34%DBApp.dataPageSize))-1;
		DBApp.select("ta", pageSelectPointer19,recordSelectPointer19);
	
		DBApp.select("ta");
	
		String[] ConditionColumns111 = {"b"};
		String[] ConditionColumnsValues111 = {"b1"};
		DBApp.select("ta", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"b"};
		String[] ConditionColumnsValues112 = {"b0"};
		DBApp.select("ta", ConditionColumns112, ConditionColumnsValues112);
	
		DBApp.select("ta");
	
		String[] ConditionColumns114 = {"a"};
		String[] ConditionColumnsValues114 = {"a10"};
		DBApp.select("ta", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"b"};
		String[] ConditionColumnsValues115 = {"b1"};
		DBApp.select("ta", ConditionColumns115, ConditionColumnsValues115);
	
		String[] ConditionColumns116 = {"a"};
		String[] ConditionColumnsValues116 = {"a18"};
		DBApp.select("ta", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b1"};
		DBApp.select("ta", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((34%DBApp.dataPageSize))-1;
		DBApp.select("ta", pageSelectPointer118,recordSelectPointer118);
	
		String[] ConditionColumns119 = {"b"};
		String[] ConditionColumnsValues119 = {"b0"};
		DBApp.select("ta", ConditionColumns119, ConditionColumnsValues119);
	
		String[] ConditionColumns120 = {"b"};
		String[] ConditionColumnsValues120 = {"b1"};
		DBApp.select("ta", ConditionColumns120, ConditionColumnsValues120);
	
		DBApp.select("ta");
	
		String selectTrace1 = DBApp.getFullTrace("ta");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 58);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 34"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("hvp", cols0);
		String [][] records_hvp = new String[303][cols0.length];
		for(int i=0;i<303;i++)
		{
			records_hvp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hvp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hvp", records_hvp[i]);
		}
	
		int pageCount0 = (int)Math.ceil(303.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		DBApp.select("hvp");
	
		String[] ConditionColumns01 = {"c","e"};
		String[] ConditionColumnsValues01 = {"c2","e1"};
		DBApp.select("hvp", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"e"};
		String[] ConditionColumnsValues02 = {"e0"};
		DBApp.select("hvp", ConditionColumns02, ConditionColumnsValues02);
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b0"};
		DBApp.select("hvp", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d0"};
		DBApp.select("hvp", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"c"};
		String[] ConditionColumnsValues05 = {"c2"};
		DBApp.select("hvp", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("hvp");
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c1"};
		DBApp.select("hvp", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("hvp");
	
		DBApp.select("hvp");
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("hvp", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("hvp", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"b"};
		String[] ConditionColumnsValues012 = {"b1"};
		DBApp.select("hvp", ConditionColumns012, ConditionColumnsValues012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("hvp", pageSelectPointer013,recordSelectPointer013);
	
		String[] ConditionColumns014 = {"e","c"};
		String[] ConditionColumnsValues014 = {"e2","c0"};
		DBApp.select("hvp", ConditionColumns014, ConditionColumnsValues014);
	
		String[] ConditionColumns015 = {"a"};
		String[] ConditionColumnsValues015 = {"a51"};
		DBApp.select("hvp", ConditionColumns015, ConditionColumnsValues015);
	
		String[] ConditionColumns016 = {"b"};
		String[] ConditionColumnsValues016 = {"b0"};
		DBApp.select("hvp", ConditionColumns016, ConditionColumnsValues016);
	
		DBApp.select("hvp");
	
		String[] ConditionColumns018 = {"d"};
		String[] ConditionColumnsValues018 = {"d0"};
		DBApp.select("hvp", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"e"};
		String[] ConditionColumnsValues019 = {"e4"};
		DBApp.select("hvp", ConditionColumns019, ConditionColumnsValues019);
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("hvp", pageSelectPointer020,recordSelectPointer020);
	
		String selectTrace0 = DBApp.getFullTrace("hvp");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 326);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 303"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("igs4x", cols1);
		String [][] records_igs4x = new String[391][cols1.length];
		for(int i=0;i<391;i++)
		{
			records_igs4x[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_igs4x[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("igs4x", records_igs4x[i]);
		}
	
		int pageCount1 = (int)Math.ceil(391.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		String[] ConditionColumns10 = {"g"};
		String[] ConditionColumnsValues10 = {"g6"};
		DBApp.select("igs4x", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"e","d"};
		String[] ConditionColumnsValues11 = {"e2","d1"};
		DBApp.select("igs4x", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("igs4x", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("igs4x", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("igs4x", pageSelectPointer14,recordSelectPointer14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((391%DBApp.dataPageSize))-1;
		DBApp.select("igs4x", pageSelectPointer15,recordSelectPointer15);
	
		String selectTrace1 = DBApp.getFullTrace("igs4x");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 399);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 391"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("al", cols2);
		String [][] records_al = new String[133][cols2.length];
		for(int i=0;i<133;i++)
		{
			records_al[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_al[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("al", records_al[i]);
		}
	
		int pageCount2 = (int)Math.ceil(133.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		String[] ConditionColumns20 = {"c","k","l"};
		String[] ConditionColumnsValues20 = {"c2","k3","l11"};
		DBApp.select("al", ConditionColumns20, ConditionColumnsValues20);
	
		String selectTrace2 = DBApp.getFullTrace("al");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 136);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 133"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("u7", cols3);
		String [][] records_u7 = new String[405][cols3.length];
		for(int i=0;i<405;i++)
		{
			records_u7[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_u7[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("u7", records_u7[i]);
		}
	
		int pageCount3 = (int)Math.ceil(405.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		DBApp.select("u7");
	
		String[] ConditionColumns31 = {"d"};
		String[] ConditionColumnsValues31 = {"d0"};
		DBApp.select("u7", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"d"};
		String[] ConditionColumnsValues32 = {"d3"};
		DBApp.select("u7", ConditionColumns32, ConditionColumnsValues32);
	
		String[] ConditionColumns33 = {"g"};
		String[] ConditionColumnsValues33 = {"g2"};
		DBApp.select("u7", ConditionColumns33, ConditionColumnsValues33);
	
		DBApp.select("u7");
	
		String[] ConditionColumns35 = {"a","d","e"};
		String[] ConditionColumnsValues35 = {"a117","d1","e2"};
		DBApp.select("u7", ConditionColumns35, ConditionColumnsValues35);
	
		DBApp.select("u7");
	
		DBApp.select("u7");
	
		String[] ConditionColumns38 = {"d","f","e"};
		String[] ConditionColumnsValues38 = {"d2","f2","e0"};
		DBApp.select("u7", ConditionColumns38, ConditionColumnsValues38);
	
		String[] ConditionColumns39 = {"b","f"};
		String[] ConditionColumnsValues39 = {"b0","f0"};
		DBApp.select("u7", ConditionColumns39, ConditionColumnsValues39);
	
		String[] ConditionColumns310 = {"a"};
		String[] ConditionColumnsValues310 = {"a276"};
		DBApp.select("u7", ConditionColumns310, ConditionColumnsValues310);
	
		DBApp.select("u7");
	
		int pageSelectPointer312 = genRandNum(pageCount3)-1;
		int recordSelectPointer312 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer312==pageCount3-1)
			recordSelectPointer312 = genRandNum((405%DBApp.dataPageSize))-1;
		DBApp.select("u7", pageSelectPointer312,recordSelectPointer312);
	
		String[] ConditionColumns313 = {"d","g","c"};
		String[] ConditionColumnsValues313 = {"d2","g5","c0"};
		DBApp.select("u7", ConditionColumns313, ConditionColumnsValues313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((405%DBApp.dataPageSize))-1;
		DBApp.select("u7", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"g","f","a"};
		String[] ConditionColumnsValues315 = {"g5","f2","a320"};
		DBApp.select("u7", ConditionColumns315, ConditionColumnsValues315);
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((405%DBApp.dataPageSize))-1;
		DBApp.select("u7", pageSelectPointer316,recordSelectPointer316);
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((405%DBApp.dataPageSize))-1;
		DBApp.select("u7", pageSelectPointer317,recordSelectPointer317);
	
		int pageSelectPointer318 = genRandNum(pageCount3)-1;
		int recordSelectPointer318 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer318==pageCount3-1)
			recordSelectPointer318 = genRandNum((405%DBApp.dataPageSize))-1;
		DBApp.select("u7", pageSelectPointer318,recordSelectPointer318);
	
		String[] ConditionColumns319 = {"g"};
		String[] ConditionColumnsValues319 = {"g5"};
		DBApp.select("u7", ConditionColumns319, ConditionColumnsValues319);
	
		String[] ConditionColumns320 = {"e"};
		String[] ConditionColumnsValues320 = {"e0"};
		DBApp.select("u7", ConditionColumns320, ConditionColumnsValues320);
	
		DBApp.select("u7");
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((405%DBApp.dataPageSize))-1;
		DBApp.select("u7", pageSelectPointer322,recordSelectPointer322);
	
		DBApp.select("u7");
	
		DBApp.select("u7");
	
		String selectTrace3 = DBApp.getFullTrace("u7");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 432);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 405"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("q5v", cols4);
		String [][] records_q5v = new String[364][cols4.length];
		for(int i=0;i<364;i++)
		{
			records_q5v[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_q5v[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("q5v", records_q5v[i]);
		}
	
		int pageCount4 = (int)Math.ceil(364.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((364%DBApp.dataPageSize))-1;
		DBApp.select("q5v", pageSelectPointer40,recordSelectPointer40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((364%DBApp.dataPageSize))-1;
		DBApp.select("q5v", pageSelectPointer41,recordSelectPointer41);
	
		DBApp.select("q5v");
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((364%DBApp.dataPageSize))-1;
		DBApp.select("q5v", pageSelectPointer43,recordSelectPointer43);
	
		int pageSelectPointer44 = genRandNum(pageCount4)-1;
		int recordSelectPointer44 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer44==pageCount4-1)
			recordSelectPointer44 = genRandNum((364%DBApp.dataPageSize))-1;
		DBApp.select("q5v", pageSelectPointer44,recordSelectPointer44);
	
		DBApp.select("q5v");
	
		DBApp.select("q5v");
	
		int pageSelectPointer47 = genRandNum(pageCount4)-1;
		int recordSelectPointer47 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer47==pageCount4-1)
			recordSelectPointer47 = genRandNum((364%DBApp.dataPageSize))-1;
		DBApp.select("q5v", pageSelectPointer47,recordSelectPointer47);
	
		DBApp.select("q5v");
	
		DBApp.select("q5v");
	
		DBApp.select("q5v");
	
		String[] ConditionColumns411 = {"c","d"};
		String[] ConditionColumnsValues411 = {"c1","d1"};
		DBApp.select("q5v", ConditionColumns411, ConditionColumnsValues411);
	
		String[] ConditionColumns412 = {"d"};
		String[] ConditionColumnsValues412 = {"d3"};
		DBApp.select("q5v", ConditionColumns412, ConditionColumnsValues412);
	
		String[] ConditionColumns413 = {"c"};
		String[] ConditionColumnsValues413 = {"c1"};
		DBApp.select("q5v", ConditionColumns413, ConditionColumnsValues413);
	
		String[] ConditionColumns414 = {"d"};
		String[] ConditionColumnsValues414 = {"d0"};
		DBApp.select("q5v", ConditionColumns414, ConditionColumnsValues414);
	
		int pageSelectPointer415 = genRandNum(pageCount4)-1;
		int recordSelectPointer415 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer415==pageCount4-1)
			recordSelectPointer415 = genRandNum((364%DBApp.dataPageSize))-1;
		DBApp.select("q5v", pageSelectPointer415,recordSelectPointer415);
	
		DBApp.select("q5v");
	
		String selectTrace4 = DBApp.getFullTrace("q5v");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 383);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 364"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("e4", cols0);
		String [][] records_e4 = new String[284][cols0.length];
		for(int i=0;i<284;i++)
		{
			records_e4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e4", records_e4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(284.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		String[] ConditionColumns00 = {"h","f","d","k","j","c"};
		String[] ConditionColumnsValues00 = {"h4","f2","d0","k5","j6","c2"};
		DBApp.select("e4", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"c"};
		String[] ConditionColumnsValues01 = {"c2"};
		DBApp.select("e4", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("e4", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("e4");
	
		DBApp.select("e4");
	
		String[] ConditionColumns05 = {"h","e","g","l"};
		String[] ConditionColumnsValues05 = {"h4","e3","g5","l8"};
		DBApp.select("e4", ConditionColumns05, ConditionColumnsValues05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("e4", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("e4", pageSelectPointer07,recordSelectPointer07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("e4", pageSelectPointer08,recordSelectPointer08);
	
		String[] ConditionColumns09 = {"g","b","m","i","e"};
		String[] ConditionColumnsValues09 = {"g3","b1","m10","i4","e3"};
		DBApp.select("e4", ConditionColumns09, ConditionColumnsValues09);
	
		DBApp.select("e4");
	
		DBApp.select("e4");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("e4", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"j","d","i"};
		String[] ConditionColumnsValues013 = {"j5","d1","i3"};
		DBApp.select("e4", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((284%DBApp.dataPageSize))-1;
		DBApp.select("e4", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"c"};
		String[] ConditionColumnsValues015 = {"c2"};
		DBApp.select("e4", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("e4");
	
		String selectTrace0 = DBApp.getFullTrace("e4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 303);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 284"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g5t", cols1);
		String [][] records_g5t = new String[439][cols1.length];
		for(int i=0;i<439;i++)
		{
			records_g5t[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g5t[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g5t", records_g5t[i]);
		}
	
		int pageCount1 = (int)Math.ceil(439.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("g5t");
	
		DBApp.select("g5t");
	
		DBApp.select("g5t");
	
		DBApp.select("g5t");
	
		DBApp.select("g5t");
	
		DBApp.select("g5t");
	
		DBApp.select("g5t");
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"d","b"};
		String[] ConditionColumnsValues19 = {"d1","b1"};
		DBApp.select("g5t", ConditionColumns19, ConditionColumnsValues19);
	
		DBApp.select("g5t");
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"f"};
		String[] ConditionColumnsValues114 = {"f0"};
		DBApp.select("g5t", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"a","f"};
		String[] ConditionColumnsValues115 = {"a49","f1"};
		DBApp.select("g5t", ConditionColumns115, ConditionColumnsValues115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer116,recordSelectPointer116);
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((439%DBApp.dataPageSize))-1;
		DBApp.select("g5t", pageSelectPointer117,recordSelectPointer117);
	
		String selectTrace1 = DBApp.getFullTrace("g5t");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 459);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 439"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("m1c4", cols2);
		String [][] records_m1c4 = new String[42][cols2.length];
		for(int i=0;i<42;i++)
		{
			records_m1c4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m1c4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m1c4", records_m1c4[i]);
		}
	
		int pageCount2 = (int)Math.ceil(42.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		DBApp.select("m1c4");
	
		DBApp.select("m1c4");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer23,recordSelectPointer23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer24,recordSelectPointer24);
	
		String[] ConditionColumns25 = {"k","e","b","f"};
		String[] ConditionColumnsValues25 = {"k9","e1","b1","f1"};
		DBApp.select("m1c4", ConditionColumns25, ConditionColumnsValues25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("m1c4");
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer28,recordSelectPointer28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer29,recordSelectPointer29);
	
		String[] ConditionColumns210 = {"c"};
		String[] ConditionColumnsValues210 = {"c0"};
		DBApp.select("m1c4", ConditionColumns210, ConditionColumnsValues210);
	
		DBApp.select("m1c4");
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"i"};
		String[] ConditionColumnsValues215 = {"i5"};
		DBApp.select("m1c4", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("m1c4");
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer217,recordSelectPointer217);
	
		DBApp.select("m1c4");
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer219,recordSelectPointer219);
	
		String[] ConditionColumns220 = {"l"};
		String[] ConditionColumnsValues220 = {"l1"};
		DBApp.select("m1c4", ConditionColumns220, ConditionColumnsValues220);
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer221,recordSelectPointer221);
	
		DBApp.select("m1c4");
	
		String[] ConditionColumns223 = {"f"};
		String[] ConditionColumnsValues223 = {"f1"};
		DBApp.select("m1c4", ConditionColumns223, ConditionColumnsValues223);
	
		String[] ConditionColumns224 = {"k"};
		String[] ConditionColumnsValues224 = {"k4"};
		DBApp.select("m1c4", ConditionColumns224, ConditionColumnsValues224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer225,recordSelectPointer225);
	
		String[] ConditionColumns226 = {"h","k","c","l"};
		String[] ConditionColumnsValues226 = {"h6","k6","c0","l6"};
		DBApp.select("m1c4", ConditionColumns226, ConditionColumnsValues226);
	
		int pageSelectPointer227 = genRandNum(pageCount2)-1;
		int recordSelectPointer227 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer227==pageCount2-1)
			recordSelectPointer227 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("m1c4", pageSelectPointer227,recordSelectPointer227);
	
		String[] ConditionColumns228 = {"c","k"};
		String[] ConditionColumnsValues228 = {"c1","k1"};
		DBApp.select("m1c4", ConditionColumns228, ConditionColumnsValues228);
	
		String selectTrace2 = DBApp.getFullTrace("m1c4");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 73);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 42"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("xuk", cols0);
		String [][] records_xuk = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			records_xuk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_xuk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("xuk", records_xuk[i]);
		}
	
		int pageCount0 = (int)Math.ceil(4.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns00 = {"e","f","d"};
		String[] ConditionColumnsValues00 = {"e2","f2","d2"};
		DBApp.select("xuk", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("xuk");
	
		DBApp.select("xuk");
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("xuk");
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("xuk");
	
		DBApp.select("xuk");
	
		DBApp.select("xuk");
	
		String[] ConditionColumns011 = {"b"};
		String[] ConditionColumnsValues011 = {"b1"};
		DBApp.select("xuk", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("xuk");
	
		String[] ConditionColumns013 = {"b"};
		String[] ConditionColumnsValues013 = {"b1"};
		DBApp.select("xuk", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("xuk");
	
		String[] ConditionColumns015 = {"a"};
		String[] ConditionColumnsValues015 = {"a3"};
		DBApp.select("xuk", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"f"};
		String[] ConditionColumnsValues017 = {"f3"};
		DBApp.select("xuk", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"c"};
		String[] ConditionColumnsValues018 = {"c1"};
		DBApp.select("xuk", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("xuk");
	
		String[] ConditionColumns020 = {"h"};
		String[] ConditionColumnsValues020 = {"h2"};
		DBApp.select("xuk", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer021,recordSelectPointer021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer022,recordSelectPointer022);
	
		DBApp.select("xuk");
	
		int pageSelectPointer024 = genRandNum(pageCount0)-1;
		int recordSelectPointer024 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer024==pageCount0-1)
			recordSelectPointer024 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer024,recordSelectPointer024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer025,recordSelectPointer025);
	
		DBApp.select("xuk");
	
		String[] ConditionColumns027 = {"e","d","h","g"};
		String[] ConditionColumnsValues027 = {"e3","d3","h3","g3"};
		DBApp.select("xuk", ConditionColumns027, ConditionColumnsValues027);
	
		int pageSelectPointer028 = genRandNum(pageCount0)-1;
		int recordSelectPointer028 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer028==pageCount0-1)
			recordSelectPointer028 = genRandNum((4%DBApp.dataPageSize))-1;
		DBApp.select("xuk", pageSelectPointer028,recordSelectPointer028);
	
		String selectTrace0 = DBApp.getFullTrace("xuk");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 35);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 4"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ejj06", cols0);
		String [][] records_ejj06 = new String[223][cols0.length];
		for(int i=0;i<223;i++)
		{
			records_ejj06[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ejj06[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ejj06", records_ejj06[i]);
		}
	
		int pageCount0 = (int)Math.ceil(223.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("ejj06");
	
		String[] ConditionColumns01 = {"a"};
		String[] ConditionColumnsValues01 = {"a22"};
		DBApp.select("ejj06", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("ejj06");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"j","k","b"};
		String[] ConditionColumnsValues04 = {"j8","k1","b0"};
		DBApp.select("ejj06", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"j"};
		String[] ConditionColumnsValues06 = {"j3"};
		DBApp.select("ejj06", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"k","h"};
		String[] ConditionColumnsValues07 = {"k8","h3"};
		DBApp.select("ejj06", ConditionColumns07, ConditionColumnsValues07);
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer08,recordSelectPointer08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"b","d","c","g"};
		String[] ConditionColumnsValues010 = {"b0","d0","c0","g1"};
		DBApp.select("ejj06", ConditionColumns010, ConditionColumnsValues010);
	
		String[] ConditionColumns011 = {"b","g","k","e","d"};
		String[] ConditionColumnsValues011 = {"b0","g6","k6","e1","d2"};
		DBApp.select("ejj06", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("ejj06");
	
		DBApp.select("ejj06");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((223%DBApp.dataPageSize))-1;
		DBApp.select("ejj06", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("ejj06");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 242);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 223"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("j9", cols1);
		String [][] records_j9 = new String[133][cols1.length];
		for(int i=0;i<133;i++)
		{
			records_j9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j9", records_j9[i]);
		}
	
		int pageCount1 = (int)Math.ceil(133.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("j9");
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("j9", pageSelectPointer11,recordSelectPointer11);
	
		String[] ConditionColumns12 = {"i","c","j","b"};
		String[] ConditionColumnsValues12 = {"i8","c2","j7","b1"};
		DBApp.select("j9", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"i","b","d","g","l","c"};
		String[] ConditionColumnsValues13 = {"i6","b1","d3","g1","l3","c0"};
		DBApp.select("j9", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b1"};
		DBApp.select("j9", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("j9", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("j9", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("j9", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("j9", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"e","l"};
		String[] ConditionColumnsValues19 = {"e1","l3"};
		DBApp.select("j9", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"g"};
		String[] ConditionColumnsValues110 = {"g3"};
		DBApp.select("j9", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("j9");
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((133%DBApp.dataPageSize))-1;
		DBApp.select("j9", pageSelectPointer112,recordSelectPointer112);
	
		DBApp.select("j9");
	
		String[] ConditionColumns114 = {"d","i","f"};
		String[] ConditionColumnsValues114 = {"d2","i6","f0"};
		DBApp.select("j9", ConditionColumns114, ConditionColumnsValues114);
	
		String selectTrace1 = DBApp.getFullTrace("j9");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 150);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 133"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("o7a", cols2);
		String [][] records_o7a = new String[180][cols2.length];
		for(int i=0;i<180;i++)
		{
			records_o7a[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_o7a[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("o7a", records_o7a[i]);
		}
	
		int pageCount2 = (int)Math.ceil(180.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		String[] ConditionColumns20 = {"g"};
		String[] ConditionColumnsValues20 = {"g2"};
		DBApp.select("o7a", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("o7a", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("o7a");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((180%DBApp.dataPageSize))-1;
		DBApp.select("o7a", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"g","e","c","h","l","o"};
		String[] ConditionColumnsValues24 = {"g0","e0","c0","h1","l9","o0"};
		DBApp.select("o7a", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("o7a");
	
		String selectTrace2 = DBApp.getFullTrace("o7a");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 188);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 180"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("r21m", cols3);
		String [][] records_r21m = new String[102][cols3.length];
		for(int i=0;i<102;i++)
		{
			records_r21m[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_r21m[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("r21m", records_r21m[i]);
		}
	
		int pageCount3 = (int)Math.ceil(102.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		DBApp.select("r21m");
	
		String[] ConditionColumns31 = {"g"};
		String[] ConditionColumnsValues31 = {"g6"};
		DBApp.select("r21m", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"b"};
		String[] ConditionColumnsValues32 = {"b0"};
		DBApp.select("r21m", ConditionColumns32, ConditionColumnsValues32);
	
		String selectTrace3 = DBApp.getFullTrace("r21m");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 107);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 102"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d89", cols0);
		String [][] records_d89 = new String[188][cols0.length];
		for(int i=0;i<188;i++)
		{
			records_d89[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_d89[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("d89", records_d89[i]);
		}
	
		int pageCount0 = (int)Math.ceil(188.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		String[] ConditionColumns00 = {"e"};
		String[] ConditionColumnsValues00 = {"e3"};
		DBApp.select("d89", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"g"};
		String[] ConditionColumnsValues01 = {"g4"};
		DBApp.select("d89", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"d","c","f"};
		String[] ConditionColumnsValues02 = {"d3","c1","f1"};
		DBApp.select("d89", ConditionColumns02, ConditionColumnsValues02);
	
		DBApp.select("d89");
	
		String[] ConditionColumns04 = {"d"};
		String[] ConditionColumnsValues04 = {"d2"};
		DBApp.select("d89", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"f","a","c"};
		String[] ConditionColumnsValues05 = {"f2","a68","c2"};
		DBApp.select("d89", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("d89");
	
		DBApp.select("d89");
	
		String[] ConditionColumns08 = {"c"};
		String[] ConditionColumnsValues08 = {"c2"};
		DBApp.select("d89", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"g","b","a"};
		String[] ConditionColumnsValues011 = {"g2","b0","a16"};
		DBApp.select("d89", ConditionColumns011, ConditionColumnsValues011);
	
		DBApp.select("d89");
	
		String[] ConditionColumns013 = {"d","b","f"};
		String[] ConditionColumnsValues013 = {"d2","b0","f2"};
		DBApp.select("d89", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"e"};
		String[] ConditionColumnsValues015 = {"e4"};
		DBApp.select("d89", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"e","b"};
		String[] ConditionColumnsValues017 = {"e4","b1"};
		DBApp.select("d89", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"e"};
		String[] ConditionColumnsValues018 = {"e0"};
		DBApp.select("d89", ConditionColumns018, ConditionColumnsValues018);
	
		String[] ConditionColumns019 = {"e"};
		String[] ConditionColumnsValues019 = {"e3"};
		DBApp.select("d89", ConditionColumns019, ConditionColumnsValues019);
	
		DBApp.select("d89");
	
		String[] ConditionColumns021 = {"e","a","f"};
		String[] ConditionColumnsValues021 = {"e3","a58","f4"};
		DBApp.select("d89", ConditionColumns021, ConditionColumnsValues021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer022,recordSelectPointer022);
	
		String[] ConditionColumns023 = {"e","a","c"};
		String[] ConditionColumnsValues023 = {"e2","a177","c0"};
		DBApp.select("d89", ConditionColumns023, ConditionColumnsValues023);
	
		String[] ConditionColumns024 = {"d"};
		String[] ConditionColumnsValues024 = {"d3"};
		DBApp.select("d89", ConditionColumns024, ConditionColumnsValues024);
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer025,recordSelectPointer025);
	
		int pageSelectPointer026 = genRandNum(pageCount0)-1;
		int recordSelectPointer026 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer026==pageCount0-1)
			recordSelectPointer026 = genRandNum((188%DBApp.dataPageSize))-1;
		DBApp.select("d89", pageSelectPointer026,recordSelectPointer026);
	
		DBApp.select("d89");
	
		String selectTrace0 = DBApp.getFullTrace("d89");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 218);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 188"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("m09", cols1);
		String [][] records_m09 = new String[289][cols1.length];
		for(int i=0;i<289;i++)
		{
			records_m09[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m09[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m09", records_m09[i]);
		}
	
		int pageCount1 = (int)Math.ceil(289.0/DBApp.dataPageSize);
	
		 //performing 15 selects:
	
		DBApp.select("m09");
	
		String[] ConditionColumns11 = {"i"};
		String[] ConditionColumnsValues11 = {"i1"};
		DBApp.select("m09", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((289%DBApp.dataPageSize))-1;
		DBApp.select("m09", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("m09");
	
		DBApp.select("m09");
	
		String[] ConditionColumns15 = {"b","e"};
		String[] ConditionColumnsValues15 = {"b1","e4"};
		DBApp.select("m09", ConditionColumns15, ConditionColumnsValues15);
	
		DBApp.select("m09");
	
		String[] ConditionColumns17 = {"a"};
		String[] ConditionColumnsValues17 = {"a279"};
		DBApp.select("m09", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("m09");
	
		DBApp.select("m09");
	
		String[] ConditionColumns110 = {"g","c"};
		String[] ConditionColumnsValues110 = {"g3","c1"};
		DBApp.select("m09", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"h"};
		String[] ConditionColumnsValues111 = {"h7"};
		DBApp.select("m09", ConditionColumns111, ConditionColumnsValues111);
	
		DBApp.select("m09");
	
		String[] ConditionColumns113 = {"e","a","b"};
		String[] ConditionColumnsValues113 = {"e3","a238","b0"};
		DBApp.select("m09", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"d","b"};
		String[] ConditionColumnsValues114 = {"d0","b0"};
		DBApp.select("m09", ConditionColumns114, ConditionColumnsValues114);
	
		String selectTrace1 = DBApp.getFullTrace("m09");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 306);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 289"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("jb", cols2);
		String [][] records_jb = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			records_jb[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jb[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jb", records_jb[i]);
		}
	
		int pageCount2 = (int)Math.ceil(25.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns20 = {"l"};
		String[] ConditionColumnsValues20 = {"l0"};
		DBApp.select("jb", ConditionColumns20, ConditionColumnsValues20);
	
		DBApp.select("jb");
	
		DBApp.select("jb");
	
		String[] ConditionColumns23 = {"k"};
		String[] ConditionColumnsValues23 = {"k5"};
		DBApp.select("jb", ConditionColumns23, ConditionColumnsValues23);
	
		String[] ConditionColumns24 = {"i","b","j"};
		String[] ConditionColumnsValues24 = {"i6","b0","j4"};
		DBApp.select("jb", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"e"};
		String[] ConditionColumnsValues25 = {"e0"};
		DBApp.select("jb", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"m"};
		String[] ConditionColumnsValues26 = {"m9"};
		DBApp.select("jb", ConditionColumns26, ConditionColumnsValues26);
	
		String[] ConditionColumns27 = {"g","e","j","n","b","d"};
		String[] ConditionColumnsValues27 = {"g1","e2","j2","n8","b0","d2"};
		DBApp.select("jb", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("jb");
	
		String[] ConditionColumns29 = {"c","b","e","d"};
		String[] ConditionColumnsValues29 = {"c1","b1","e3","d1"};
		DBApp.select("jb", ConditionColumns29, ConditionColumnsValues29);
	
		DBApp.select("jb");
	
		String[] ConditionColumns211 = {"a"};
		String[] ConditionColumnsValues211 = {"a11"};
		DBApp.select("jb", ConditionColumns211, ConditionColumnsValues211);
	
		String[] ConditionColumns212 = {"c"};
		String[] ConditionColumnsValues212 = {"c0"};
		DBApp.select("jb", ConditionColumns212, ConditionColumnsValues212);
	
		DBApp.select("jb");
	
		String[] ConditionColumns214 = {"l","j"};
		String[] ConditionColumnsValues214 = {"l1","j3"};
		DBApp.select("jb", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"a"};
		String[] ConditionColumnsValues215 = {"a12"};
		DBApp.select("jb", ConditionColumns215, ConditionColumnsValues215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((25%DBApp.dataPageSize))-1;
		DBApp.select("jb", pageSelectPointer216,recordSelectPointer216);
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((25%DBApp.dataPageSize))-1;
		DBApp.select("jb", pageSelectPointer217,recordSelectPointer217);
	
		String[] ConditionColumns218 = {"e"};
		String[] ConditionColumnsValues218 = {"e3"};
		DBApp.select("jb", ConditionColumns218, ConditionColumnsValues218);
	
		String[] ConditionColumns219 = {"l","c"};
		String[] ConditionColumnsValues219 = {"l11","c2"};
		DBApp.select("jb", ConditionColumns219, ConditionColumnsValues219);
	
		DBApp.select("jb");
	
		DBApp.select("jb");
	
		String selectTrace2 = DBApp.getFullTrace("jb");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 49);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 25"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("btf", cols0);
		String [][] records_btf = new String[333][cols0.length];
		for(int i=0;i<333;i++)
		{
			records_btf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_btf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("btf", records_btf[i]);
		}
	
		int pageCount0 = (int)Math.ceil(333.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		DBApp.select("btf");
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e2"};
		DBApp.select("btf", ConditionColumns01, ConditionColumnsValues01);
	
		DBApp.select("btf");
	
		DBApp.select("btf");
	
		DBApp.select("btf");
	
		DBApp.select("btf");
	
		DBApp.select("btf");
	
		DBApp.select("btf");
	
		String[] ConditionColumns08 = {"b"};
		String[] ConditionColumnsValues08 = {"b0"};
		DBApp.select("btf", ConditionColumns08, ConditionColumnsValues08);
	
		String selectTrace0 = DBApp.getFullTrace("btf");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 344);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 333"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t757", cols1);
		String [][] records_t757 = new String[209][cols1.length];
		for(int i=0;i<209;i++)
		{
			records_t757[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t757[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t757", records_t757[i]);
		}
	
		int pageCount1 = (int)Math.ceil(209.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("t757", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("t757");
	
		String[] ConditionColumns12 = {"f","m"};
		String[] ConditionColumnsValues12 = {"f5","m5"};
		DBApp.select("t757", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"e","h","k","g","j","i"};
		String[] ConditionColumnsValues13 = {"e1","h7","k1","g6","j1","i3"};
		DBApp.select("t757", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c","g"};
		String[] ConditionColumnsValues14 = {"c1","g4"};
		DBApp.select("t757", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((209%DBApp.dataPageSize))-1;
		DBApp.select("t757", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("t757");
	
		String[] ConditionColumns17 = {"j","f","n","k","e","a"};
		String[] ConditionColumnsValues17 = {"j8","f4","n0","k6","e3","a28"};
		DBApp.select("t757", ConditionColumns17, ConditionColumnsValues17);
	
		String[] ConditionColumns18 = {"n"};
		String[] ConditionColumnsValues18 = {"n7"};
		DBApp.select("t757", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("t757");
	
		DBApp.select("t757");
	
		DBApp.select("t757");
	
		String selectTrace1 = DBApp.getFullTrace("t757");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 223);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 209"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("vq20g", cols2);
		String [][] records_vq20g = new String[142][cols2.length];
		for(int i=0;i<142;i++)
		{
			records_vq20g[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vq20g[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vq20g", records_vq20g[i]);
		}
	
		int pageCount2 = (int)Math.ceil(142.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("vq20g", pageSelectPointer20,recordSelectPointer20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("vq20g", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"b"};
		String[] ConditionColumnsValues22 = {"b0"};
		DBApp.select("vq20g", ConditionColumns22, ConditionColumnsValues22);
	
		String selectTrace2 = DBApp.getFullTrace("vq20g");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 147);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 142"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q9y", cols3);
		String [][] records_q9y = new String[368][cols3.length];
		for(int i=0;i<368;i++)
		{
			records_q9y[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q9y[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q9y", records_q9y[i]);
		}
	
		int pageCount3 = (int)Math.ceil(368.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("q9y", pageSelectPointer30,recordSelectPointer30);
	
		String[] ConditionColumns31 = {"l"};
		String[] ConditionColumnsValues31 = {"l10"};
		DBApp.select("q9y", ConditionColumns31, ConditionColumnsValues31);
	
		String[] ConditionColumns32 = {"l"};
		String[] ConditionColumnsValues32 = {"l0"};
		DBApp.select("q9y", ConditionColumns32, ConditionColumnsValues32);
	
		DBApp.select("q9y");
	
		DBApp.select("q9y");
	
		DBApp.select("q9y");
	
		DBApp.select("q9y");
	
		DBApp.select("q9y");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("q9y", pageSelectPointer38,recordSelectPointer38);
	
		String[] ConditionColumns39 = {"i"};
		String[] ConditionColumnsValues39 = {"i1"};
		DBApp.select("q9y", ConditionColumns39, ConditionColumnsValues39);
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("q9y", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("q9y");
	
		String[] ConditionColumns312 = {"g","k","a","j"};
		String[] ConditionColumnsValues312 = {"g6","k0","a132","j2"};
		DBApp.select("q9y", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((368%DBApp.dataPageSize))-1;
		DBApp.select("q9y", pageSelectPointer313,recordSelectPointer313);
	
		String[] ConditionColumns314 = {"g","f"};
		String[] ConditionColumnsValues314 = {"g1","f2"};
		DBApp.select("q9y", ConditionColumns314, ConditionColumnsValues314);
	
		DBApp.select("q9y");
	
		String selectTrace3 = DBApp.getFullTrace("q9y");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 386);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 368"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("z2", cols4);
		String [][] records_z2 = new String[303][cols4.length];
		for(int i=0;i<303;i++)
		{
			records_z2[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_z2[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("z2", records_z2[i]);
		}
	
		int pageCount4 = (int)Math.ceil(303.0/DBApp.dataPageSize);
	
		 //performing 4 selects:
	
		String[] ConditionColumns40 = {"i","c","e","k","a","l","g"};
		String[] ConditionColumnsValues40 = {"i7","c1","e1","k6","a61","l1","g5"};
		DBApp.select("z2", ConditionColumns40, ConditionColumnsValues40);
	
		String[] ConditionColumns41 = {"g","c"};
		String[] ConditionColumnsValues41 = {"g6","c0"};
		DBApp.select("z2", ConditionColumns41, ConditionColumnsValues41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((303%DBApp.dataPageSize))-1;
		DBApp.select("z2", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"c","m"};
		String[] ConditionColumnsValues43 = {"c2","m6"};
		DBApp.select("z2", ConditionColumns43, ConditionColumnsValues43);
	
		String selectTrace4 = DBApp.getFullTrace("z2");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 309);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 303"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("iw", cols0);
		String [][] records_iw = new String[442][cols0.length];
		for(int i=0;i<442;i++)
		{
			records_iw[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_iw[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("iw", records_iw[i]);
		}
	
		int pageCount0 = (int)Math.ceil(442.0/DBApp.dataPageSize);
	
		 //performing 1 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((442%DBApp.dataPageSize))-1;
		DBApp.select("iw", pageSelectPointer00,recordSelectPointer00);
	
		String selectTrace0 = DBApp.getFullTrace("iw");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 445);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 442"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("m3", cols1);
		String [][] records_m3 = new String[139][cols1.length];
		for(int i=0;i<139;i++)
		{
			records_m3[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m3[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m3", records_m3[i]);
		}
	
		int pageCount1 = (int)Math.ceil(139.0/DBApp.dataPageSize);
	
		 //performing 13 selects:
	
		String[] ConditionColumns10 = {"b"};
		String[] ConditionColumnsValues10 = {"b0"};
		DBApp.select("m3", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"c"};
		String[] ConditionColumnsValues11 = {"c0"};
		DBApp.select("m3", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((139%DBApp.dataPageSize))-1;
		DBApp.select("m3", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("m3");
	
		DBApp.select("m3");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((139%DBApp.dataPageSize))-1;
		DBApp.select("m3", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("m3");
	
		DBApp.select("m3");
	
		DBApp.select("m3");
	
		DBApp.select("m3");
	
		String[] ConditionColumns110 = {"c"};
		String[] ConditionColumnsValues110 = {"c1"};
		DBApp.select("m3", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("m3");
	
		DBApp.select("m3");
	
		String selectTrace1 = DBApp.getFullTrace("m3");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 154);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 139"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("yj83", cols0);
		String [][] records_yj83 = new String[90][cols0.length];
		for(int i=0;i<90;i++)
		{
			records_yj83[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yj83[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yj83", records_yj83[i]);
		}
	
		int pageCount0 = (int)Math.ceil(90.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer00,recordSelectPointer00);
	
		DBApp.select("yj83");
	
		DBApp.select("yj83");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("yj83");
	
		DBApp.select("yj83");
	
		String[] ConditionColumns06 = {"a"};
		String[] ConditionColumnsValues06 = {"a86"};
		DBApp.select("yj83", ConditionColumns06, ConditionColumnsValues06);
	
		DBApp.select("yj83");
	
		DBApp.select("yj83");
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer011,recordSelectPointer011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer012,recordSelectPointer012);
	
		DBApp.select("yj83");
	
		String[] ConditionColumns014 = {"a"};
		String[] ConditionColumnsValues014 = {"a69"};
		DBApp.select("yj83", ConditionColumns014, ConditionColumnsValues014);
	
		DBApp.select("yj83");
	
		DBApp.select("yj83");
	
		DBApp.select("yj83");
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((90%DBApp.dataPageSize))-1;
		DBApp.select("yj83", pageSelectPointer018,recordSelectPointer018);
	
		String selectTrace0 = DBApp.getFullTrace("yj83");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 111);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 90"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jk2", cols0);
		String [][] records_jk2 = new String[103][cols0.length];
		for(int i=0;i<103;i++)
		{
			records_jk2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jk2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jk2", records_jk2[i]);
		}
	
		int pageCount0 = (int)Math.ceil(103.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer02,recordSelectPointer02);
	
		String[] ConditionColumns03 = {"c"};
		String[] ConditionColumnsValues03 = {"c0"};
		DBApp.select("jk2", ConditionColumns03, ConditionColumnsValues03);
	
		String[] ConditionColumns04 = {"e"};
		String[] ConditionColumnsValues04 = {"e0"};
		DBApp.select("jk2", ConditionColumns04, ConditionColumnsValues04);
	
		DBApp.select("jk2");
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer06,recordSelectPointer06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"i","l","j"};
		String[] ConditionColumnsValues08 = {"i4","l10","j8"};
		DBApp.select("jk2", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer09,recordSelectPointer09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((103%DBApp.dataPageSize))-1;
		DBApp.select("jk2", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"i"};
		String[] ConditionColumnsValues012 = {"i1"};
		DBApp.select("jk2", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("jk2");
	
		String selectTrace0 = DBApp.getFullTrace("jk2");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 119);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 103"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ue", cols1);
		String [][] records_ue = new String[237][cols1.length];
		for(int i=0;i<237;i++)
		{
			records_ue[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ue[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ue", records_ue[i]);
		}
	
		int pageCount1 = (int)Math.ceil(237.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		String[] ConditionColumns10 = {"g","n","o"};
		String[] ConditionColumnsValues10 = {"g6","n13","o9"};
		DBApp.select("ue", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"h"};
		String[] ConditionColumnsValues11 = {"h0"};
		DBApp.select("ue", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"g"};
		String[] ConditionColumnsValues12 = {"g3"};
		DBApp.select("ue", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("ue");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("ue", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("ue");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("ue", pageSelectPointer16,recordSelectPointer16);
	
		String[] ConditionColumns17 = {"j","h","n","e","b"};
		String[] ConditionColumnsValues17 = {"j5","h1","n5","e0","b1"};
		DBApp.select("ue", ConditionColumns17, ConditionColumnsValues17);
	
		DBApp.select("ue");
	
		String[] ConditionColumns19 = {"m","n","k","l","f"};
		String[] ConditionColumnsValues19 = {"m8","n8","k3","l10","f4"};
		DBApp.select("ue", ConditionColumns19, ConditionColumnsValues19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((237%DBApp.dataPageSize))-1;
		DBApp.select("ue", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("ue");
	
		String selectTrace1 = DBApp.getFullTrace("ue");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 251);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 237"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("m0e", cols2);
		String [][] records_m0e = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			records_m0e[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m0e[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m0e", records_m0e[i]);
		}
	
		int pageCount2 = (int)Math.ceil(96.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("m0e");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("m0e");
	
		DBApp.select("m0e");
	
		String[] ConditionColumns24 = {"e"};
		String[] ConditionColumnsValues24 = {"e4"};
		DBApp.select("m0e", ConditionColumns24, ConditionColumnsValues24);
	
		String[] ConditionColumns25 = {"g","h"};
		String[] ConditionColumnsValues25 = {"g2","h2"};
		DBApp.select("m0e", ConditionColumns25, ConditionColumnsValues25);
	
		String[] ConditionColumns26 = {"c"};
		String[] ConditionColumnsValues26 = {"c1"};
		DBApp.select("m0e", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer27,recordSelectPointer27);
	
		int pageSelectPointer28 = genRandNum(pageCount2)-1;
		int recordSelectPointer28 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer28==pageCount2-1)
			recordSelectPointer28 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer28,recordSelectPointer28);
	
		DBApp.select("m0e");
	
		String[] ConditionColumns210 = {"b"};
		String[] ConditionColumnsValues210 = {"b0"};
		DBApp.select("m0e", ConditionColumns210, ConditionColumnsValues210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer211,recordSelectPointer211);
	
		String[] ConditionColumns212 = {"h"};
		String[] ConditionColumnsValues212 = {"h6"};
		DBApp.select("m0e", ConditionColumns212, ConditionColumnsValues212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("m0e");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer215,recordSelectPointer215);
	
		int pageSelectPointer216 = genRandNum(pageCount2)-1;
		int recordSelectPointer216 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer216==pageCount2-1)
			recordSelectPointer216 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer216,recordSelectPointer216);
	
		String[] ConditionColumns217 = {"e"};
		String[] ConditionColumnsValues217 = {"e1"};
		DBApp.select("m0e", ConditionColumns217, ConditionColumnsValues217);
	
		String[] ConditionColumns218 = {"b"};
		String[] ConditionColumnsValues218 = {"b0"};
		DBApp.select("m0e", ConditionColumns218, ConditionColumnsValues218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer219,recordSelectPointer219);
	
		DBApp.select("m0e");
	
		int pageSelectPointer221 = genRandNum(pageCount2)-1;
		int recordSelectPointer221 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer221==pageCount2-1)
			recordSelectPointer221 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer221,recordSelectPointer221);
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer222,recordSelectPointer222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer223,recordSelectPointer223);
	
		String[] ConditionColumns224 = {"d"};
		String[] ConditionColumnsValues224 = {"d1"};
		DBApp.select("m0e", ConditionColumns224, ConditionColumnsValues224);
	
		int pageSelectPointer225 = genRandNum(pageCount2)-1;
		int recordSelectPointer225 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer225==pageCount2-1)
			recordSelectPointer225 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer225,recordSelectPointer225);
	
		DBApp.select("m0e");
	
		int pageSelectPointer227 = genRandNum(pageCount2)-1;
		int recordSelectPointer227 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer227==pageCount2-1)
			recordSelectPointer227 = genRandNum((96%DBApp.dataPageSize))-1;
		DBApp.select("m0e", pageSelectPointer227,recordSelectPointer227);
	
		DBApp.select("m0e");
	
		String[] ConditionColumns229 = {"b"};
		String[] ConditionColumnsValues229 = {"b1"};
		DBApp.select("m0e", ConditionColumns229, ConditionColumnsValues229);
	
		String selectTrace2 = DBApp.getFullTrace("m0e");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 128);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 96"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("p5", cols3);
		String [][] records_p5 = new String[52][cols3.length];
		for(int i=0;i<52;i++)
		{
			records_p5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_p5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("p5", records_p5[i]);
		}
	
		int pageCount3 = (int)Math.ceil(52.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		DBApp.select("p5");
	
		DBApp.select("p5");
	
		DBApp.select("p5");
	
		String[] ConditionColumns33 = {"d","a"};
		String[] ConditionColumnsValues33 = {"d3","a51"};
		DBApp.select("p5", ConditionColumns33, ConditionColumnsValues33);
	
		int pageSelectPointer34 = genRandNum(pageCount3)-1;
		int recordSelectPointer34 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer34==pageCount3-1)
			recordSelectPointer34 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer34,recordSelectPointer34);
	
		DBApp.select("p5");
	
		DBApp.select("p5");
	
		String[] ConditionColumns37 = {"e"};
		String[] ConditionColumnsValues37 = {"e3"};
		DBApp.select("p5", ConditionColumns37, ConditionColumnsValues37);
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer38,recordSelectPointer38);
	
		DBApp.select("p5");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((52%DBApp.dataPageSize))-1;
		DBApp.select("p5", pageSelectPointer310,recordSelectPointer310);
	
		String selectTrace3 = DBApp.getFullTrace("p5");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 65);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 52"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a"};
		DBApp.createTable("r1ywn", cols4);
		String [][] records_r1ywn = new String[273][cols4.length];
		for(int i=0;i<273;i++)
		{
			records_r1ywn[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_r1ywn[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("r1ywn", records_r1ywn[i]);
		}
	
		int pageCount4 = (int)Math.ceil(273.0/DBApp.dataPageSize);
	
		 //performing 10 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((273%DBApp.dataPageSize))-1;
		DBApp.select("r1ywn", pageSelectPointer40,recordSelectPointer40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((273%DBApp.dataPageSize))-1;
		DBApp.select("r1ywn", pageSelectPointer41,recordSelectPointer41);
	
		DBApp.select("r1ywn");
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((273%DBApp.dataPageSize))-1;
		DBApp.select("r1ywn", pageSelectPointer43,recordSelectPointer43);
	
		DBApp.select("r1ywn");
	
		DBApp.select("r1ywn");
	
		int pageSelectPointer46 = genRandNum(pageCount4)-1;
		int recordSelectPointer46 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer46==pageCount4-1)
			recordSelectPointer46 = genRandNum((273%DBApp.dataPageSize))-1;
		DBApp.select("r1ywn", pageSelectPointer46,recordSelectPointer46);
	
		String[] ConditionColumns47 = {"a"};
		String[] ConditionColumnsValues47 = {"a161"};
		DBApp.select("r1ywn", ConditionColumns47, ConditionColumnsValues47);
	
		int pageSelectPointer48 = genRandNum(pageCount4)-1;
		int recordSelectPointer48 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer48==pageCount4-1)
			recordSelectPointer48 = genRandNum((273%DBApp.dataPageSize))-1;
		DBApp.select("r1ywn", pageSelectPointer48,recordSelectPointer48);
	
		String[] ConditionColumns49 = {"a"};
		String[] ConditionColumnsValues49 = {"a219"};
		DBApp.select("r1ywn", ConditionColumns49, ConditionColumnsValues49);
	
		String selectTrace4 = DBApp.getFullTrace("r1ywn");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 285);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 273"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("zh", cols0);
		String [][] records_zh = new String[488][cols0.length];
		for(int i=0;i<488;i++)
		{
			records_zh[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_zh[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("zh", records_zh[i]);
		}
	
		int pageCount0 = (int)Math.ceil(488.0/DBApp.dataPageSize);
	
		 //performing 17 selects:
	
		DBApp.select("zh");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((488%DBApp.dataPageSize))-1;
		DBApp.select("zh", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("zh");
	
		DBApp.select("zh");
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a108"};
		DBApp.select("zh", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((488%DBApp.dataPageSize))-1;
		DBApp.select("zh", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("zh");
	
		DBApp.select("zh");
	
		DBApp.select("zh");
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a436"};
		DBApp.select("zh", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((488%DBApp.dataPageSize))-1;
		DBApp.select("zh", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("zh");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((488%DBApp.dataPageSize))-1;
		DBApp.select("zh", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((488%DBApp.dataPageSize))-1;
		DBApp.select("zh", pageSelectPointer013,recordSelectPointer013);
	
		DBApp.select("zh");
	
		DBApp.select("zh");
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((488%DBApp.dataPageSize))-1;
		DBApp.select("zh", pageSelectPointer016,recordSelectPointer016);
	
		String selectTrace0 = DBApp.getFullTrace("zh");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 507);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 488"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v3un", cols0);
		String [][] records_v3un = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			records_v3un[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v3un[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v3un", records_v3un[i]);
		}
	
		int pageCount0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
	
		 //performing 14 selects:
	
		String[] ConditionColumns00 = {"d","c"};
		String[] ConditionColumnsValues00 = {"d2","c1"};
		DBApp.select("v3un", ConditionColumns00, ConditionColumnsValues00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((35%DBApp.dataPageSize))-1;
		DBApp.select("v3un", pageSelectPointer01,recordSelectPointer01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((35%DBApp.dataPageSize))-1;
		DBApp.select("v3un", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("v3un");
	
		String[] ConditionColumns04 = {"a"};
		String[] ConditionColumnsValues04 = {"a2"};
		DBApp.select("v3un", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((35%DBApp.dataPageSize))-1;
		DBApp.select("v3un", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"g"};
		String[] ConditionColumnsValues06 = {"g6"};
		DBApp.select("v3un", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"a","c","d","h","g"};
		String[] ConditionColumnsValues07 = {"a31","c1","d3","h7","g3"};
		DBApp.select("v3un", ConditionColumns07, ConditionColumnsValues07);
	
		DBApp.select("v3un");
	
		String[] ConditionColumns09 = {"a"};
		String[] ConditionColumnsValues09 = {"a25"};
		DBApp.select("v3un", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((35%DBApp.dataPageSize))-1;
		DBApp.select("v3un", pageSelectPointer010,recordSelectPointer010);
	
		DBApp.select("v3un");
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((35%DBApp.dataPageSize))-1;
		DBApp.select("v3un", pageSelectPointer012,recordSelectPointer012);
	
		String[] ConditionColumns013 = {"f","e","g"};
		String[] ConditionColumnsValues013 = {"f5","e4","g1"};
		DBApp.select("v3un", ConditionColumns013, ConditionColumnsValues013);
	
		String selectTrace0 = DBApp.getFullTrace("v3un");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 51);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 35"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jpf", cols0);
		String [][] records_jpf = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			records_jpf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jpf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jpf", records_jpf[i]);
		}
	
		int pageCount0 = (int)Math.ceil(42.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		DBApp.select("jpf");
	
		String[] ConditionColumns01 = {"g","b"};
		String[] ConditionColumnsValues01 = {"g6","b1"};
		DBApp.select("jpf", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("jpf", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("jpf");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("jpf", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("jpf", pageSelectPointer05,recordSelectPointer05);
	
		int pageSelectPointer06 = genRandNum(pageCount0)-1;
		int recordSelectPointer06 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer06==pageCount0-1)
			recordSelectPointer06 = genRandNum((42%DBApp.dataPageSize))-1;
		DBApp.select("jpf", pageSelectPointer06,recordSelectPointer06);
	
		DBApp.select("jpf");
	
		String selectTrace0 = DBApp.getFullTrace("jpf");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 52);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 42"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("s3u", cols1);
		String [][] records_s3u = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_s3u[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s3u[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s3u", records_s3u[i]);
		}
	
		int pageCount1 = (int)Math.ceil(47.0/DBApp.dataPageSize);
	
		 //performing 20 selects:
	
		DBApp.select("s3u");
	
		String[] ConditionColumns11 = {"f"};
		String[] ConditionColumnsValues11 = {"f4"};
		DBApp.select("s3u", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("s3u", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("s3u", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("s3u");
	
		DBApp.select("s3u");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("s3u", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("s3u", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"g","b","e","h"};
		String[] ConditionColumnsValues18 = {"g1","b0","e3","h0"};
		DBApp.select("s3u", ConditionColumns18, ConditionColumnsValues18);
	
		String[] ConditionColumns19 = {"h"};
		String[] ConditionColumnsValues19 = {"h1"};
		DBApp.select("s3u", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"a","c","j","i","b"};
		String[] ConditionColumnsValues110 = {"a17","c2","j7","i8","b1"};
		DBApp.select("s3u", ConditionColumns110, ConditionColumnsValues110);
	
		DBApp.select("s3u");
	
		DBApp.select("s3u");
	
		DBApp.select("s3u");
	
		DBApp.select("s3u");
	
		String[] ConditionColumns115 = {"j","f","g","b"};
		String[] ConditionColumnsValues115 = {"j7","f5","g3","b1"};
		DBApp.select("s3u", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("s3u");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("s3u", pageSelectPointer117,recordSelectPointer117);
	
		DBApp.select("s3u");
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((47%DBApp.dataPageSize))-1;
		DBApp.select("s3u", pageSelectPointer119,recordSelectPointer119);
	
		String selectTrace1 = DBApp.getFullTrace("s3u");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 69);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 47"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ts1z", cols2);
		String [][] records_ts1z = new String[212][cols2.length];
		for(int i=0;i<212;i++)
		{
			records_ts1z[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ts1z[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ts1z", records_ts1z[i]);
		}
	
		int pageCount2 = (int)Math.ceil(212.0/DBApp.dataPageSize);
	
		 //performing 12 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((212%DBApp.dataPageSize))-1;
		DBApp.select("ts1z", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("ts1z");
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((212%DBApp.dataPageSize))-1;
		DBApp.select("ts1z", pageSelectPointer22,recordSelectPointer22);
	
		DBApp.select("ts1z");
	
		DBApp.select("ts1z");
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((212%DBApp.dataPageSize))-1;
		DBApp.select("ts1z", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"i"};
		String[] ConditionColumnsValues26 = {"i4"};
		DBApp.select("ts1z", ConditionColumns26, ConditionColumnsValues26);
	
		String[] ConditionColumns27 = {"b"};
		String[] ConditionColumnsValues27 = {"b0"};
		DBApp.select("ts1z", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"e","f"};
		String[] ConditionColumnsValues28 = {"e2","f4"};
		DBApp.select("ts1z", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("ts1z");
	
		String[] ConditionColumns210 = {"h"};
		String[] ConditionColumnsValues210 = {"h5"};
		DBApp.select("ts1z", ConditionColumns210, ConditionColumnsValues210);
	
		String[] ConditionColumns211 = {"g"};
		String[] ConditionColumnsValues211 = {"g1"};
		DBApp.select("ts1z", ConditionColumns211, ConditionColumnsValues211);
	
		String selectTrace2 = DBApp.getFullTrace("ts1z");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 226);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 212"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("o0", cols0);
		String [][] records_o0 = new String[120][cols0.length];
		for(int i=0;i<120;i++)
		{
			records_o0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o0", records_o0[i]);
		}
	
		int pageCount0 = (int)Math.ceil(120.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a22"};
		DBApp.select("o0", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"c"};
		String[] ConditionColumnsValues01 = {"c2"};
		DBApp.select("o0", ConditionColumns01, ConditionColumnsValues01);
	
		String selectTrace0 = DBApp.getFullTrace("o0");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 124);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 120"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v0", cols1);
		String [][] records_v0 = new String[207][cols1.length];
		for(int i=0;i<207;i++)
		{
			records_v0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v0", records_v0[i]);
		}
	
		int pageCount1 = (int)Math.ceil(207.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns10 = {"n"};
		String[] ConditionColumnsValues10 = {"n5"};
		DBApp.select("v0", ConditionColumns10, ConditionColumnsValues10);
	
		String[] ConditionColumns11 = {"a","n","d","f","m"};
		String[] ConditionColumnsValues11 = {"a137","n11","d1","f5","m7"};
		DBApp.select("v0", ConditionColumns11, ConditionColumnsValues11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("v0");
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer14,recordSelectPointer14);
	
		String[] ConditionColumns15 = {"c","k","j"};
		String[] ConditionColumnsValues15 = {"c1","k2","j2"};
		DBApp.select("v0", ConditionColumns15, ConditionColumnsValues15);
	
		String[] ConditionColumns16 = {"h","n","l"};
		String[] ConditionColumnsValues16 = {"h6","n6","l10"};
		DBApp.select("v0", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"h"};
		String[] ConditionColumnsValues18 = {"h7"};
		DBApp.select("v0", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer19,recordSelectPointer19);
	
		String[] ConditionColumns110 = {"c"};
		String[] ConditionColumnsValues110 = {"c1"};
		DBApp.select("v0", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("v0");
	
		String[] ConditionColumns113 = {"i","g","f","n"};
		String[] ConditionColumnsValues113 = {"i3","g1","f3","n1"};
		DBApp.select("v0", ConditionColumns113, ConditionColumnsValues113);
	
		String[] ConditionColumns114 = {"f"};
		String[] ConditionColumnsValues114 = {"f1"};
		DBApp.select("v0", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"b"};
		String[] ConditionColumnsValues115 = {"b1"};
		DBApp.select("v0", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("v0");
	
		int pageSelectPointer117 = genRandNum(pageCount1)-1;
		int recordSelectPointer117 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer117==pageCount1-1)
			recordSelectPointer117 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer117,recordSelectPointer117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer118,recordSelectPointer118);
	
		int pageSelectPointer119 = genRandNum(pageCount1)-1;
		int recordSelectPointer119 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer119==pageCount1-1)
			recordSelectPointer119 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer119,recordSelectPointer119);
	
		DBApp.select("v0");
	
		DBApp.select("v0");
	
		int pageSelectPointer122 = genRandNum(pageCount1)-1;
		int recordSelectPointer122 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer122==pageCount1-1)
			recordSelectPointer122 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer122,recordSelectPointer122);
	
		DBApp.select("v0");
	
		int pageSelectPointer124 = genRandNum(pageCount1)-1;
		int recordSelectPointer124 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer124==pageCount1-1)
			recordSelectPointer124 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer124,recordSelectPointer124);
	
		String[] ConditionColumns125 = {"i"};
		String[] ConditionColumnsValues125 = {"i5"};
		DBApp.select("v0", ConditionColumns125, ConditionColumnsValues125);
	
		DBApp.select("v0");
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((207%DBApp.dataPageSize))-1;
		DBApp.select("v0", pageSelectPointer127,recordSelectPointer127);
	
		String[] ConditionColumns128 = {"c"};
		String[] ConditionColumnsValues128 = {"c0"};
		DBApp.select("v0", ConditionColumns128, ConditionColumnsValues128);
	
		String selectTrace1 = DBApp.getFullTrace("v0");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 238);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 207"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("qy8", cols0);
		String [][] records_qy8 = new String[183][cols0.length];
		for(int i=0;i<183;i++)
		{
			records_qy8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_qy8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("qy8", records_qy8[i]);
		}
	
		int pageCount0 = (int)Math.ceil(183.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		DBApp.select("qy8");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((183%DBApp.dataPageSize))-1;
		DBApp.select("qy8", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("qy8");
	
		String[] ConditionColumns03 = {"b"};
		String[] ConditionColumnsValues03 = {"b1"};
		DBApp.select("qy8", ConditionColumns03, ConditionColumnsValues03);
	
		DBApp.select("qy8");
	
		String selectTrace0 = DBApp.getFullTrace("qy8");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 190);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 183"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("y6suz", cols1);
		String [][] records_y6suz = new String[134][cols1.length];
		for(int i=0;i<134;i++)
		{
			records_y6suz[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_y6suz[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("y6suz", records_y6suz[i]);
		}
	
		int pageCount1 = (int)Math.ceil(134.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		String[] ConditionColumns10 = {"a"};
		String[] ConditionColumnsValues10 = {"a25"};
		DBApp.select("y6suz", ConditionColumns10, ConditionColumnsValues10);
	
		DBApp.select("y6suz");
	
		String[] ConditionColumns12 = {"c"};
		String[] ConditionColumnsValues12 = {"c1"};
		DBApp.select("y6suz", ConditionColumns12, ConditionColumnsValues12);
	
		String[] ConditionColumns13 = {"a","d"};
		String[] ConditionColumnsValues13 = {"a112","d0"};
		DBApp.select("y6suz", ConditionColumns13, ConditionColumnsValues13);
	
		String[] ConditionColumns14 = {"c"};
		String[] ConditionColumnsValues14 = {"c2"};
		DBApp.select("y6suz", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("y6suz");
	
		String[] ConditionColumns17 = {"b"};
		String[] ConditionColumnsValues17 = {"b0"};
		DBApp.select("y6suz", ConditionColumns17, ConditionColumnsValues17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer18,recordSelectPointer18);
	
		String[] ConditionColumns19 = {"e"};
		String[] ConditionColumnsValues19 = {"e2"};
		DBApp.select("y6suz", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"e"};
		String[] ConditionColumnsValues110 = {"e2"};
		DBApp.select("y6suz", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"b","a"};
		String[] ConditionColumnsValues114 = {"b0","a14"};
		DBApp.select("y6suz", ConditionColumns114, ConditionColumnsValues114);
	
		DBApp.select("y6suz");
	
		String[] ConditionColumns116 = {"a"};
		String[] ConditionColumnsValues116 = {"a68"};
		DBApp.select("y6suz", ConditionColumns116, ConditionColumnsValues116);
	
		DBApp.select("y6suz");
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer118,recordSelectPointer118);
	
		DBApp.select("y6suz");
	
		DBApp.select("y6suz");
	
		DBApp.select("y6suz");
	
		String[] ConditionColumns122 = {"a","d"};
		String[] ConditionColumnsValues122 = {"a40","d0"};
		DBApp.select("y6suz", ConditionColumns122, ConditionColumnsValues122);
	
		DBApp.select("y6suz");
	
		DBApp.select("y6suz");
	
		String[] ConditionColumns125 = {"c"};
		String[] ConditionColumnsValues125 = {"c1"};
		DBApp.select("y6suz", ConditionColumns125, ConditionColumnsValues125);
	
		DBApp.select("y6suz");
	
		int pageSelectPointer127 = genRandNum(pageCount1)-1;
		int recordSelectPointer127 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer127==pageCount1-1)
			recordSelectPointer127 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer127,recordSelectPointer127);
	
		int pageSelectPointer128 = genRandNum(pageCount1)-1;
		int recordSelectPointer128 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer128==pageCount1-1)
			recordSelectPointer128 = genRandNum((134%DBApp.dataPageSize))-1;
		DBApp.select("y6suz", pageSelectPointer128,recordSelectPointer128);
	
		String selectTrace1 = DBApp.getFullTrace("y6suz");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 165);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 134"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("h5t", cols2);
		String [][] records_h5t = new String[308][cols2.length];
		for(int i=0;i<308;i++)
		{
			records_h5t[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h5t[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h5t", records_h5t[i]);
		}
	
		int pageCount2 = (int)Math.ceil(308.0/DBApp.dataPageSize);
	
		 //performing 18 selects:
	
		String[] ConditionColumns20 = {"d","e"};
		String[] ConditionColumnsValues20 = {"d1","e0"};
		DBApp.select("h5t", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((308%DBApp.dataPageSize))-1;
		DBApp.select("h5t", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("h5t");
	
		String[] ConditionColumns23 = {"b"};
		String[] ConditionColumnsValues23 = {"b0"};
		DBApp.select("h5t", ConditionColumns23, ConditionColumnsValues23);
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((308%DBApp.dataPageSize))-1;
		DBApp.select("h5t", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("h5t");
	
		DBApp.select("h5t");
	
		String[] ConditionColumns27 = {"c"};
		String[] ConditionColumnsValues27 = {"c2"};
		DBApp.select("h5t", ConditionColumns27, ConditionColumnsValues27);
	
		DBApp.select("h5t");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((308%DBApp.dataPageSize))-1;
		DBApp.select("h5t", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("h5t");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((308%DBApp.dataPageSize))-1;
		DBApp.select("h5t", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("h5t");
	
		String[] ConditionColumns213 = {"c"};
		String[] ConditionColumnsValues213 = {"c1"};
		DBApp.select("h5t", ConditionColumns213, ConditionColumnsValues213);
	
		String[] ConditionColumns214 = {"a"};
		String[] ConditionColumnsValues214 = {"a296"};
		DBApp.select("h5t", ConditionColumns214, ConditionColumnsValues214);
	
		String[] ConditionColumns215 = {"d"};
		String[] ConditionColumnsValues215 = {"d2"};
		DBApp.select("h5t", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("h5t");
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((308%DBApp.dataPageSize))-1;
		DBApp.select("h5t", pageSelectPointer217,recordSelectPointer217);
	
		String selectTrace2 = DBApp.getFullTrace("h5t");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 328);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 308"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ok9r3", cols3);
		String [][] records_ok9r3 = new String[38][cols3.length];
		for(int i=0;i<38;i++)
		{
			records_ok9r3[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_ok9r3[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("ok9r3", records_ok9r3[i]);
		}
	
		int pageCount3 = (int)Math.ceil(38.0/DBApp.dataPageSize);
	
		 //performing 11 selects:
	
		DBApp.select("ok9r3");
	
		DBApp.select("ok9r3");
	
		DBApp.select("ok9r3");
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("ok9r3", pageSelectPointer33,recordSelectPointer33);
	
		DBApp.select("ok9r3");
	
		DBApp.select("ok9r3");
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("ok9r3", pageSelectPointer36,recordSelectPointer36);
	
		int pageSelectPointer37 = genRandNum(pageCount3)-1;
		int recordSelectPointer37 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer37==pageCount3-1)
			recordSelectPointer37 = genRandNum((38%DBApp.dataPageSize))-1;
		DBApp.select("ok9r3", pageSelectPointer37,recordSelectPointer37);
	
		String[] ConditionColumns38 = {"a","h"};
		String[] ConditionColumnsValues38 = {"a32","h0"};
		DBApp.select("ok9r3", ConditionColumns38, ConditionColumnsValues38);
	
		String[] ConditionColumns39 = {"c"};
		String[] ConditionColumnsValues39 = {"c0"};
		DBApp.select("ok9r3", ConditionColumns39, ConditionColumnsValues39);
	
		DBApp.select("ok9r3");
	
		String selectTrace3 = DBApp.getFullTrace("ok9r3");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 51);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 38"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("e9939", cols0);
		String [][] records_e9939 = new String[256][cols0.length];
		for(int i=0;i<256;i++)
		{
			records_e9939[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e9939[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e9939", records_e9939[i]);
		}
	
		int pageCount0 = (int)Math.ceil(256.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"e"};
		String[] ConditionColumnsValues01 = {"e1"};
		DBApp.select("e9939", ConditionColumns01, ConditionColumnsValues01);
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("e9939");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"b","c","a"};
		String[] ConditionColumnsValues06 = {"b1","c2","a119"};
		DBApp.select("e9939", ConditionColumns06, ConditionColumnsValues06);
	
		String[] ConditionColumns07 = {"c"};
		String[] ConditionColumnsValues07 = {"c0"};
		DBApp.select("e9939", ConditionColumns07, ConditionColumnsValues07);
	
		String[] ConditionColumns08 = {"j","c"};
		String[] ConditionColumnsValues08 = {"j1","c0"};
		DBApp.select("e9939", ConditionColumns08, ConditionColumnsValues08);
	
		String[] ConditionColumns09 = {"h","j","c"};
		String[] ConditionColumnsValues09 = {"h7","j3","c1"};
		DBApp.select("e9939", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer010,recordSelectPointer010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"g"};
		String[] ConditionColumnsValues012 = {"g6"};
		DBApp.select("e9939", ConditionColumns012, ConditionColumnsValues012);
	
		String[] ConditionColumns013 = {"g","i","j"};
		String[] ConditionColumnsValues013 = {"g4","i7","j4"};
		DBApp.select("e9939", ConditionColumns013, ConditionColumnsValues013);
	
		DBApp.select("e9939");
	
		int pageSelectPointer015 = genRandNum(pageCount0)-1;
		int recordSelectPointer015 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer015==pageCount0-1)
			recordSelectPointer015 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer015,recordSelectPointer015);
	
		String[] ConditionColumns016 = {"b"};
		String[] ConditionColumnsValues016 = {"b0"};
		DBApp.select("e9939", ConditionColumns016, ConditionColumnsValues016);
	
		String[] ConditionColumns017 = {"c"};
		String[] ConditionColumnsValues017 = {"c1"};
		DBApp.select("e9939", ConditionColumns017, ConditionColumnsValues017);
	
		String[] ConditionColumns018 = {"e","i","h","k"};
		String[] ConditionColumnsValues018 = {"e3","i6","h6","k1"};
		DBApp.select("e9939", ConditionColumns018, ConditionColumnsValues018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((256%DBApp.dataPageSize))-1;
		DBApp.select("e9939", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"d","c","k","f","e"};
		String[] ConditionColumnsValues020 = {"d3","c0","k10","f3","e2"};
		DBApp.select("e9939", ConditionColumns020, ConditionColumnsValues020);
	
		String selectTrace0 = DBApp.getFullTrace("e9939");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 279);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 256"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("ej5", cols0);
		String [][] records_ej5 = new String[174][cols0.length];
		for(int i=0;i<174;i++)
		{
			records_ej5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ej5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ej5", records_ej5[i]);
		}
	
		int pageCount0 = (int)Math.ceil(174.0/DBApp.dataPageSize);
	
		 //performing 26 selects:
	
		DBApp.select("ej5");
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("ej5");
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer03,recordSelectPointer03);
	
		String[] ConditionColumns04 = {"c"};
		String[] ConditionColumnsValues04 = {"c0"};
		DBApp.select("ej5", ConditionColumns04, ConditionColumnsValues04);
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a39"};
		DBApp.select("ej5", ConditionColumns05, ConditionColumnsValues05);
	
		DBApp.select("ej5");
	
		DBApp.select("ej5");
	
		int pageSelectPointer08 = genRandNum(pageCount0)-1;
		int recordSelectPointer08 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer08==pageCount0-1)
			recordSelectPointer08 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer08,recordSelectPointer08);
	
		DBApp.select("ej5");
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"d"};
		String[] ConditionColumnsValues011 = {"d3"};
		DBApp.select("ej5", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"d"};
		String[] ConditionColumnsValues015 = {"d0"};
		DBApp.select("ej5", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("ej5");
	
		String[] ConditionColumns017 = {"c"};
		String[] ConditionColumnsValues017 = {"c1"};
		DBApp.select("ej5", ConditionColumns017, ConditionColumnsValues017);
	
		int pageSelectPointer018 = genRandNum(pageCount0)-1;
		int recordSelectPointer018 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer018==pageCount0-1)
			recordSelectPointer018 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer018,recordSelectPointer018);
	
		int pageSelectPointer019 = genRandNum(pageCount0)-1;
		int recordSelectPointer019 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer019==pageCount0-1)
			recordSelectPointer019 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer019,recordSelectPointer019);
	
		String[] ConditionColumns020 = {"b"};
		String[] ConditionColumnsValues020 = {"b1"};
		DBApp.select("ej5", ConditionColumns020, ConditionColumnsValues020);
	
		DBApp.select("ej5");
	
		String[] ConditionColumns022 = {"c","a"};
		String[] ConditionColumnsValues022 = {"c2","a92"};
		DBApp.select("ej5", ConditionColumns022, ConditionColumnsValues022);
	
		int pageSelectPointer023 = genRandNum(pageCount0)-1;
		int recordSelectPointer023 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer023==pageCount0-1)
			recordSelectPointer023 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer023,recordSelectPointer023);
	
		DBApp.select("ej5");
	
		int pageSelectPointer025 = genRandNum(pageCount0)-1;
		int recordSelectPointer025 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer025==pageCount0-1)
			recordSelectPointer025 = genRandNum((174%DBApp.dataPageSize))-1;
		DBApp.select("ej5", pageSelectPointer025,recordSelectPointer025);
	
		String selectTrace0 = DBApp.getFullTrace("ej5");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 202);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 174"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("gxl6", cols1);
		String [][] records_gxl6 = new String[247][cols1.length];
		for(int i=0;i<247;i++)
		{
			records_gxl6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gxl6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gxl6", records_gxl6[i]);
		}
	
		int pageCount1 = (int)Math.ceil(247.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("gxl6", pageSelectPointer10,recordSelectPointer10);
	
		DBApp.select("gxl6");
	
		String[] ConditionColumns12 = {"d","j","f","k"};
		String[] ConditionColumnsValues12 = {"d2","j2","f0","k9"};
		DBApp.select("gxl6", ConditionColumns12, ConditionColumnsValues12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("gxl6", pageSelectPointer13,recordSelectPointer13);
	
		String[] ConditionColumns14 = {"l","d"};
		String[] ConditionColumnsValues14 = {"l8","d0"};
		DBApp.select("gxl6", ConditionColumns14, ConditionColumnsValues14);
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("gxl6", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"m"};
		String[] ConditionColumnsValues16 = {"m11"};
		DBApp.select("gxl6", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("gxl6", pageSelectPointer17,recordSelectPointer17);
	
		DBApp.select("gxl6");
	
		String[] ConditionColumns19 = {"d"};
		String[] ConditionColumnsValues19 = {"d2"};
		DBApp.select("gxl6", ConditionColumns19, ConditionColumnsValues19);
	
		String[] ConditionColumns110 = {"m"};
		String[] ConditionColumnsValues110 = {"m0"};
		DBApp.select("gxl6", ConditionColumns110, ConditionColumnsValues110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("gxl6", pageSelectPointer111,recordSelectPointer111);
	
		DBApp.select("gxl6");
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((247%DBApp.dataPageSize))-1;
		DBApp.select("gxl6", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("gxl6");
	
		String[] ConditionColumns115 = {"m","b","f","j"};
		String[] ConditionColumnsValues115 = {"m1","b1","f3","j7"};
		DBApp.select("gxl6", ConditionColumns115, ConditionColumnsValues115);
	
		String selectTrace1 = DBApp.getFullTrace("gxl6");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 265);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 247"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("l0", cols0);
		String [][] records_l0 = new String[437][cols0.length];
		for(int i=0;i<437;i++)
		{
			records_l0[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_l0[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("l0", records_l0[i]);
		}
	
		int pageCount0 = (int)Math.ceil(437.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		String[] ConditionColumns00 = {"a"};
		String[] ConditionColumnsValues00 = {"a43"};
		DBApp.select("l0", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("l0");
	
		String[] ConditionColumns02 = {"a"};
		String[] ConditionColumnsValues02 = {"a66"};
		DBApp.select("l0", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l0", pageSelectPointer03,recordSelectPointer03);
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((437%DBApp.dataPageSize))-1;
		DBApp.select("l0", pageSelectPointer04,recordSelectPointer04);
	
		String[] ConditionColumns05 = {"a"};
		String[] ConditionColumnsValues05 = {"a93"};
		DBApp.select("l0", ConditionColumns05, ConditionColumnsValues05);
	
		String selectTrace0 = DBApp.getFullTrace("l0");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 445);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 437"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("dl", cols1);
		String [][] records_dl = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			records_dl[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dl[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dl", records_dl[i]);
		}
	
		int pageCount1 = (int)Math.ceil(78.0/DBApp.dataPageSize);
	
		 //performing 8 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dl", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dl", pageSelectPointer11,recordSelectPointer11);
	
		DBApp.select("dl");
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dl", pageSelectPointer13,recordSelectPointer13);
	
		DBApp.select("dl");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((78%DBApp.dataPageSize))-1;
		DBApp.select("dl", pageSelectPointer15,recordSelectPointer15);
	
		DBApp.select("dl");
	
		DBApp.select("dl");
	
		String selectTrace1 = DBApp.getFullTrace("dl");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 88);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 78"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("k53", cols0);
		String [][] records_k53 = new String[323][cols0.length];
		for(int i=0;i<323;i++)
		{
			records_k53[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k53[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k53", records_k53[i]);
		}
	
		int pageCount0 = (int)Math.ceil(323.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((323%DBApp.dataPageSize))-1;
		DBApp.select("k53", pageSelectPointer00,recordSelectPointer00);
	
		String[] ConditionColumns01 = {"b"};
		String[] ConditionColumnsValues01 = {"b0"};
		DBApp.select("k53", ConditionColumns01, ConditionColumnsValues01);
	
		String selectTrace0 = DBApp.getFullTrace("k53");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 327);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 323"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("cwh7", cols1);
		String [][] records_cwh7 = new String[383][cols1.length];
		for(int i=0;i<383;i++)
		{
			records_cwh7[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_cwh7[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("cwh7", records_cwh7[i]);
		}
	
		int pageCount1 = (int)Math.ceil(383.0/DBApp.dataPageSize);
	
		 //performing 9 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer12,recordSelectPointer12);
	
		String[] ConditionColumns13 = {"o"};
		String[] ConditionColumnsValues13 = {"o11"};
		DBApp.select("cwh7", ConditionColumns13, ConditionColumnsValues13);
	
		DBApp.select("cwh7");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer15,recordSelectPointer15);
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((383%DBApp.dataPageSize))-1;
		DBApp.select("cwh7", pageSelectPointer18,recordSelectPointer18);
	
		String selectTrace1 = DBApp.getFullTrace("cwh7");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 394);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 383"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("io0", cols2);
		String [][] records_io0 = new String[382][cols2.length];
		for(int i=0;i<382;i++)
		{
			records_io0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_io0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("io0", records_io0[i]);
		}
	
		int pageCount2 = (int)Math.ceil(382.0/DBApp.dataPageSize);
	
		 //performing 25 selects:
	
		String[] ConditionColumns20 = {"b"};
		String[] ConditionColumnsValues20 = {"b0"};
		DBApp.select("io0", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer21,recordSelectPointer21);
	
		String[] ConditionColumns22 = {"b"};
		String[] ConditionColumnsValues22 = {"b0"};
		DBApp.select("io0", ConditionColumns22, ConditionColumnsValues22);
	
		DBApp.select("io0");
	
		String[] ConditionColumns24 = {"b"};
		String[] ConditionColumnsValues24 = {"b1"};
		DBApp.select("io0", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer25,recordSelectPointer25);
	
		String[] ConditionColumns26 = {"a"};
		String[] ConditionColumnsValues26 = {"a278"};
		DBApp.select("io0", ConditionColumns26, ConditionColumnsValues26);
	
		String[] ConditionColumns27 = {"a"};
		String[] ConditionColumnsValues27 = {"a39"};
		DBApp.select("io0", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"b"};
		String[] ConditionColumnsValues28 = {"b1"};
		DBApp.select("io0", ConditionColumns28, ConditionColumnsValues28);
	
		String[] ConditionColumns29 = {"b"};
		String[] ConditionColumnsValues29 = {"b1"};
		DBApp.select("io0", ConditionColumns29, ConditionColumnsValues29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer210,recordSelectPointer210);
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer211,recordSelectPointer211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer212,recordSelectPointer212);
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer213,recordSelectPointer213);
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"b"};
		String[] ConditionColumnsValues215 = {"b0"};
		DBApp.select("io0", ConditionColumns215, ConditionColumnsValues215);
	
		DBApp.select("io0");
	
		String[] ConditionColumns217 = {"b"};
		String[] ConditionColumnsValues217 = {"b1"};
		DBApp.select("io0", ConditionColumns217, ConditionColumnsValues217);
	
		String[] ConditionColumns218 = {"b"};
		String[] ConditionColumnsValues218 = {"b0"};
		DBApp.select("io0", ConditionColumns218, ConditionColumnsValues218);
	
		int pageSelectPointer219 = genRandNum(pageCount2)-1;
		int recordSelectPointer219 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer219==pageCount2-1)
			recordSelectPointer219 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer219,recordSelectPointer219);
	
		int pageSelectPointer220 = genRandNum(pageCount2)-1;
		int recordSelectPointer220 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer220==pageCount2-1)
			recordSelectPointer220 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer220,recordSelectPointer220);
	
		String[] ConditionColumns221 = {"a"};
		String[] ConditionColumnsValues221 = {"a220"};
		DBApp.select("io0", ConditionColumns221, ConditionColumnsValues221);
	
		int pageSelectPointer222 = genRandNum(pageCount2)-1;
		int recordSelectPointer222 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer222==pageCount2-1)
			recordSelectPointer222 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer222,recordSelectPointer222);
	
		int pageSelectPointer223 = genRandNum(pageCount2)-1;
		int recordSelectPointer223 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer223==pageCount2-1)
			recordSelectPointer223 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer223,recordSelectPointer223);
	
		int pageSelectPointer224 = genRandNum(pageCount2)-1;
		int recordSelectPointer224 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer224==pageCount2-1)
			recordSelectPointer224 = genRandNum((382%DBApp.dataPageSize))-1;
		DBApp.select("io0", pageSelectPointer224,recordSelectPointer224);
	
		String selectTrace2 = DBApp.getFullTrace("io0");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 409);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 382"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x8b93", cols3);
		String [][] records_x8b93 = new String[109][cols3.length];
		for(int i=0;i<109;i++)
		{
			records_x8b93[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_x8b93[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("x8b93", records_x8b93[i]);
		}
	
		int pageCount3 = (int)Math.ceil(109.0/DBApp.dataPageSize);
	
		 //performing 30 selects:
	
		DBApp.select("x8b93");
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer31,recordSelectPointer31);
	
		String[] ConditionColumns32 = {"e"};
		String[] ConditionColumnsValues32 = {"e3"};
		DBApp.select("x8b93", ConditionColumns32, ConditionColumnsValues32);
	
		int pageSelectPointer33 = genRandNum(pageCount3)-1;
		int recordSelectPointer33 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer33==pageCount3-1)
			recordSelectPointer33 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer33,recordSelectPointer33);
	
		DBApp.select("x8b93");
	
		String[] ConditionColumns35 = {"d","b","f"};
		String[] ConditionColumnsValues35 = {"d1","b1","f5"};
		DBApp.select("x8b93", ConditionColumns35, ConditionColumnsValues35);
	
		String[] ConditionColumns36 = {"e"};
		String[] ConditionColumnsValues36 = {"e2"};
		DBApp.select("x8b93", ConditionColumns36, ConditionColumnsValues36);
	
		String[] ConditionColumns37 = {"c"};
		String[] ConditionColumnsValues37 = {"c0"};
		DBApp.select("x8b93", ConditionColumns37, ConditionColumnsValues37);
	
		String[] ConditionColumns38 = {"g","e"};
		String[] ConditionColumnsValues38 = {"g0","e2"};
		DBApp.select("x8b93", ConditionColumns38, ConditionColumnsValues38);
	
		DBApp.select("x8b93");
	
		String[] ConditionColumns310 = {"d"};
		String[] ConditionColumnsValues310 = {"d2"};
		DBApp.select("x8b93", ConditionColumns310, ConditionColumnsValues310);
	
		String[] ConditionColumns311 = {"f"};
		String[] ConditionColumnsValues311 = {"f4"};
		DBApp.select("x8b93", ConditionColumns311, ConditionColumnsValues311);
	
		String[] ConditionColumns312 = {"g"};
		String[] ConditionColumnsValues312 = {"g0"};
		DBApp.select("x8b93", ConditionColumns312, ConditionColumnsValues312);
	
		DBApp.select("x8b93");
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"a"};
		String[] ConditionColumnsValues315 = {"a96"};
		DBApp.select("x8b93", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("x8b93");
	
		DBApp.select("x8b93");
	
		DBApp.select("x8b93");
	
		int pageSelectPointer319 = genRandNum(pageCount3)-1;
		int recordSelectPointer319 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer319==pageCount3-1)
			recordSelectPointer319 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer319,recordSelectPointer319);
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer320,recordSelectPointer320);
	
		String[] ConditionColumns321 = {"c"};
		String[] ConditionColumnsValues321 = {"c2"};
		DBApp.select("x8b93", ConditionColumns321, ConditionColumnsValues321);
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer322,recordSelectPointer322);
	
		DBApp.select("x8b93");
	
		DBApp.select("x8b93");
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((109%DBApp.dataPageSize))-1;
		DBApp.select("x8b93", pageSelectPointer325,recordSelectPointer325);
	
		String[] ConditionColumns326 = {"c"};
		String[] ConditionColumnsValues326 = {"c0"};
		DBApp.select("x8b93", ConditionColumns326, ConditionColumnsValues326);
	
		DBApp.select("x8b93");
	
		DBApp.select("x8b93");
	
		String[] ConditionColumns329 = {"b"};
		String[] ConditionColumnsValues329 = {"b1"};
		DBApp.select("x8b93", ConditionColumns329, ConditionColumnsValues329);
	
		String selectTrace3 = DBApp.getFullTrace("x8b93");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 141);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 109"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("i5635", cols0);
		String [][] records_i5635 = new String[495][cols0.length];
		for(int i=0;i<495;i++)
		{
			records_i5635[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_i5635[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("i5635", records_i5635[i]);
		}
	
		int pageCount0 = (int)Math.ceil(495.0/DBApp.dataPageSize);
	
		 //performing 3 selects:
	
		int pageSelectPointer00 = genRandNum(pageCount0)-1;
		int recordSelectPointer00 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer00==pageCount0-1)
			recordSelectPointer00 = genRandNum((495%DBApp.dataPageSize))-1;
		DBApp.select("i5635", pageSelectPointer00,recordSelectPointer00);
	
		int pageSelectPointer01 = genRandNum(pageCount0)-1;
		int recordSelectPointer01 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer01==pageCount0-1)
			recordSelectPointer01 = genRandNum((495%DBApp.dataPageSize))-1;
		DBApp.select("i5635", pageSelectPointer01,recordSelectPointer01);
	
		DBApp.select("i5635");
	
		String selectTrace0 = DBApp.getFullTrace("i5635");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 500);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 495"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yk", cols1);
		String [][] records_yk = new String[434][cols1.length];
		for(int i=0;i<434;i++)
		{
			records_yk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_yk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("yk", records_yk[i]);
		}
	
		int pageCount1 = (int)Math.ceil(434.0/DBApp.dataPageSize);
	
		 //performing 28 selects:
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		int pageSelectPointer15 = genRandNum(pageCount1)-1;
		int recordSelectPointer15 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer15==pageCount1-1)
			recordSelectPointer15 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer15,recordSelectPointer15);
	
		String[] ConditionColumns16 = {"c","h","b"};
		String[] ConditionColumnsValues16 = {"c0","h0","b0"};
		DBApp.select("yk", ConditionColumns16, ConditionColumnsValues16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer17,recordSelectPointer17);
	
		String[] ConditionColumns18 = {"h"};
		String[] ConditionColumnsValues18 = {"h1"};
		DBApp.select("yk", ConditionColumns18, ConditionColumnsValues18);
	
		int pageSelectPointer19 = genRandNum(pageCount1)-1;
		int recordSelectPointer19 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer19==pageCount1-1)
			recordSelectPointer19 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer19,recordSelectPointer19);
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer110,recordSelectPointer110);
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		int pageSelectPointer114 = genRandNum(pageCount1)-1;
		int recordSelectPointer114 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer114==pageCount1-1)
			recordSelectPointer114 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer114,recordSelectPointer114);
	
		DBApp.select("yk");
	
		String[] ConditionColumns116 = {"f"};
		String[] ConditionColumnsValues116 = {"f5"};
		DBApp.select("yk", ConditionColumns116, ConditionColumnsValues116);
	
		String[] ConditionColumns117 = {"b"};
		String[] ConditionColumnsValues117 = {"b0"};
		DBApp.select("yk", ConditionColumns117, ConditionColumnsValues117);
	
		int pageSelectPointer118 = genRandNum(pageCount1)-1;
		int recordSelectPointer118 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer118==pageCount1-1)
			recordSelectPointer118 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer118,recordSelectPointer118);
	
		String[] ConditionColumns119 = {"a"};
		String[] ConditionColumnsValues119 = {"a259"};
		DBApp.select("yk", ConditionColumns119, ConditionColumnsValues119);
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer121,recordSelectPointer121);
	
		String[] ConditionColumns122 = {"d"};
		String[] ConditionColumnsValues122 = {"d1"};
		DBApp.select("yk", ConditionColumns122, ConditionColumnsValues122);
	
		String[] ConditionColumns123 = {"e"};
		String[] ConditionColumnsValues123 = {"e3"};
		DBApp.select("yk", ConditionColumns123, ConditionColumnsValues123);
	
		String[] ConditionColumns124 = {"e","h"};
		String[] ConditionColumnsValues124 = {"e1","h6"};
		DBApp.select("yk", ConditionColumns124, ConditionColumnsValues124);
	
		int pageSelectPointer125 = genRandNum(pageCount1)-1;
		int recordSelectPointer125 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer125==pageCount1-1)
			recordSelectPointer125 = genRandNum((434%DBApp.dataPageSize))-1;
		DBApp.select("yk", pageSelectPointer125,recordSelectPointer125);
	
		DBApp.select("yk");
	
		DBApp.select("yk");
	
		String selectTrace1 = DBApp.getFullTrace("yk");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 464);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 434"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("mjuy", cols2);
		String [][] records_mjuy = new String[485][cols2.length];
		for(int i=0;i<485;i++)
		{
			records_mjuy[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_mjuy[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("mjuy", records_mjuy[i]);
		}
	
		int pageCount2 = (int)Math.ceil(485.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		String[] ConditionColumns20 = {"d"};
		String[] ConditionColumnsValues20 = {"d1"};
		DBApp.select("mjuy", ConditionColumns20, ConditionColumnsValues20);
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("mjuy");
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"e","a","d","n","k"};
		String[] ConditionColumnsValues24 = {"e1","a296","d0","n2","k10"};
		DBApp.select("mjuy", ConditionColumns24, ConditionColumnsValues24);
	
		int pageSelectPointer25 = genRandNum(pageCount2)-1;
		int recordSelectPointer25 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer25==pageCount2-1)
			recordSelectPointer25 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer25,recordSelectPointer25);
	
		int pageSelectPointer26 = genRandNum(pageCount2)-1;
		int recordSelectPointer26 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer26==pageCount2-1)
			recordSelectPointer26 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer26,recordSelectPointer26);
	
		DBApp.select("mjuy");
	
		DBApp.select("mjuy");
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer29,recordSelectPointer29);
	
		int pageSelectPointer210 = genRandNum(pageCount2)-1;
		int recordSelectPointer210 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer210==pageCount2-1)
			recordSelectPointer210 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer210,recordSelectPointer210);
	
		String[] ConditionColumns211 = {"k","h","i"};
		String[] ConditionColumnsValues211 = {"k3","h0","i0"};
		DBApp.select("mjuy", ConditionColumns211, ConditionColumnsValues211);
	
		int pageSelectPointer212 = genRandNum(pageCount2)-1;
		int recordSelectPointer212 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer212==pageCount2-1)
			recordSelectPointer212 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer212,recordSelectPointer212);
	
		DBApp.select("mjuy");
	
		int pageSelectPointer214 = genRandNum(pageCount2)-1;
		int recordSelectPointer214 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer214==pageCount2-1)
			recordSelectPointer214 = genRandNum((485%DBApp.dataPageSize))-1;
		DBApp.select("mjuy", pageSelectPointer214,recordSelectPointer214);
	
		String[] ConditionColumns215 = {"a","o"};
		String[] ConditionColumnsValues215 = {"a326","o11"};
		DBApp.select("mjuy", ConditionColumns215, ConditionColumnsValues215);
	
		String[] ConditionColumns216 = {"d"};
		String[] ConditionColumnsValues216 = {"d3"};
		DBApp.select("mjuy", ConditionColumns216, ConditionColumnsValues216);
	
		String[] ConditionColumns217 = {"i"};
		String[] ConditionColumnsValues217 = {"i5"};
		DBApp.select("mjuy", ConditionColumns217, ConditionColumnsValues217);
	
		DBApp.select("mjuy");
	
		String[] ConditionColumns219 = {"f","e","b","g","n","k","o"};
		String[] ConditionColumnsValues219 = {"f4","e0","b0","g3","n10","k10","o10"};
		DBApp.select("mjuy", ConditionColumns219, ConditionColumnsValues219);
	
		String[] ConditionColumns220 = {"c"};
		String[] ConditionColumnsValues220 = {"c0"};
		DBApp.select("mjuy", ConditionColumns220, ConditionColumnsValues220);
	
		String[] ConditionColumns221 = {"f"};
		String[] ConditionColumnsValues221 = {"f3"};
		DBApp.select("mjuy", ConditionColumns221, ConditionColumnsValues221);
	
		String selectTrace2 = DBApp.getFullTrace("mjuy");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 509);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 485"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("n74q0", cols3);
		String [][] records_n74q0 = new String[271][cols3.length];
		for(int i=0;i<271;i++)
		{
			records_n74q0[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n74q0[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n74q0", records_n74q0[i]);
		}
	
		int pageCount3 = (int)Math.ceil(271.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		String[] ConditionColumns30 = {"a"};
		String[] ConditionColumnsValues30 = {"a130"};
		DBApp.select("n74q0", ConditionColumns30, ConditionColumnsValues30);
	
		DBApp.select("n74q0");
	
		String selectTrace3 = DBApp.getFullTrace("n74q0");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 275);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 271"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("bc997", cols4);
		String [][] records_bc997 = new String[259][cols4.length];
		for(int i=0;i<259;i++)
		{
			records_bc997[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_bc997[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("bc997", records_bc997[i]);
		}
	
		int pageCount4 = (int)Math.ceil(259.0/DBApp.dataPageSize);
	
		 //performing 5 selects:
	
		int pageSelectPointer40 = genRandNum(pageCount4)-1;
		int recordSelectPointer40 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer40==pageCount4-1)
			recordSelectPointer40 = genRandNum((259%DBApp.dataPageSize))-1;
		DBApp.select("bc997", pageSelectPointer40,recordSelectPointer40);
	
		DBApp.select("bc997");
	
		String[] ConditionColumns42 = {"b"};
		String[] ConditionColumnsValues42 = {"b1"};
		DBApp.select("bc997", ConditionColumns42, ConditionColumnsValues42);
	
		int pageSelectPointer43 = genRandNum(pageCount4)-1;
		int recordSelectPointer43 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer43==pageCount4-1)
			recordSelectPointer43 = genRandNum((259%DBApp.dataPageSize))-1;
		DBApp.select("bc997", pageSelectPointer43,recordSelectPointer43);
	
		String[] ConditionColumns44 = {"d"};
		String[] ConditionColumnsValues44 = {"d0"};
		DBApp.select("bc997", ConditionColumns44, ConditionColumnsValues44);
	
		String selectTrace4 = DBApp.getFullTrace("bc997");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 266);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 259"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a74m", cols0);
		String [][] records_a74m = new String[149][cols0.length];
		for(int i=0;i<149;i++)
		{
			records_a74m[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a74m[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a74m", records_a74m[i]);
		}
	
		int pageCount0 = (int)Math.ceil(149.0/DBApp.dataPageSize);
	
		 //performing 21 selects:
	
		DBApp.select("a74m");
	
		DBApp.select("a74m");
	
		int pageSelectPointer02 = genRandNum(pageCount0)-1;
		int recordSelectPointer02 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer02==pageCount0-1)
			recordSelectPointer02 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer02,recordSelectPointer02);
	
		DBApp.select("a74m");
	
		int pageSelectPointer04 = genRandNum(pageCount0)-1;
		int recordSelectPointer04 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer04==pageCount0-1)
			recordSelectPointer04 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer04,recordSelectPointer04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer05,recordSelectPointer05);
	
		String[] ConditionColumns06 = {"e"};
		String[] ConditionColumnsValues06 = {"e1"};
		DBApp.select("a74m", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer07,recordSelectPointer07);
	
		String[] ConditionColumns08 = {"e"};
		String[] ConditionColumnsValues08 = {"e2"};
		DBApp.select("a74m", ConditionColumns08, ConditionColumnsValues08);
	
		int pageSelectPointer09 = genRandNum(pageCount0)-1;
		int recordSelectPointer09 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer09==pageCount0-1)
			recordSelectPointer09 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer09,recordSelectPointer09);
	
		String[] ConditionColumns010 = {"c"};
		String[] ConditionColumnsValues010 = {"c1"};
		DBApp.select("a74m", ConditionColumns010, ConditionColumnsValues010);
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer011,recordSelectPointer011);
	
		String[] ConditionColumns012 = {"h"};
		String[] ConditionColumnsValues012 = {"h3"};
		DBApp.select("a74m", ConditionColumns012, ConditionColumnsValues012);
	
		DBApp.select("a74m");
	
		DBApp.select("a74m");
	
		DBApp.select("a74m");
	
		DBApp.select("a74m");
	
		DBApp.select("a74m");
	
		String[] ConditionColumns018 = {"c","j","h"};
		String[] ConditionColumnsValues018 = {"c0","j0","h2"};
		DBApp.select("a74m", ConditionColumns018, ConditionColumnsValues018);
	
		DBApp.select("a74m");
	
		int pageSelectPointer020 = genRandNum(pageCount0)-1;
		int recordSelectPointer020 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer020==pageCount0-1)
			recordSelectPointer020 = genRandNum((149%DBApp.dataPageSize))-1;
		DBApp.select("a74m", pageSelectPointer020,recordSelectPointer020);
	
		String selectTrace0 = DBApp.getFullTrace("a74m");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 172);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 149"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("gl49o", cols1);
		String [][] records_gl49o = new String[308][cols1.length];
		for(int i=0;i<308;i++)
		{
			records_gl49o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_gl49o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("gl49o", records_gl49o[i]);
		}
	
		int pageCount1 = (int)Math.ceil(308.0/DBApp.dataPageSize);
	
		 //performing 6 selects:
	
		DBApp.select("gl49o");
	
		String[] ConditionColumns11 = {"d"};
		String[] ConditionColumnsValues11 = {"d3"};
		DBApp.select("gl49o", ConditionColumns11, ConditionColumnsValues11);
	
		String[] ConditionColumns12 = {"e","f"};
		String[] ConditionColumnsValues12 = {"e4","f1"};
		DBApp.select("gl49o", ConditionColumns12, ConditionColumnsValues12);
	
		DBApp.select("gl49o");
	
		String[] ConditionColumns14 = {"b"};
		String[] ConditionColumnsValues14 = {"b0"};
		DBApp.select("gl49o", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("gl49o");
	
		String selectTrace1 = DBApp.getFullTrace("gl49o");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 316);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 308"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("sl", cols2);
		String [][] records_sl = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			records_sl[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_sl[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("sl", records_sl[i]);
		}
	
		int pageCount2 = (int)Math.ceil(12.0/DBApp.dataPageSize);
	
		 //performing 2 selects:
	
		int pageSelectPointer20 = genRandNum(pageCount2)-1;
		int recordSelectPointer20 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer20==pageCount2-1)
			recordSelectPointer20 = genRandNum((12%DBApp.dataPageSize))-1;
		DBApp.select("sl", pageSelectPointer20,recordSelectPointer20);
	
		DBApp.select("sl");
	
		String selectTrace2 = DBApp.getFullTrace("sl");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 16);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 12"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("pf", cols3);
		String [][] records_pf = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			records_pf[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_pf[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("pf", records_pf[i]);
		}
	
		int pageCount3 = (int)Math.ceil(55.0/DBApp.dataPageSize);
	
		 //performing 27 selects:
	
		String[] ConditionColumns30 = {"d"};
		String[] ConditionColumnsValues30 = {"d3"};
		DBApp.select("pf", ConditionColumns30, ConditionColumnsValues30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((55%DBApp.dataPageSize))-1;
		DBApp.select("pf", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("pf");
	
		String[] ConditionColumns33 = {"b"};
		String[] ConditionColumnsValues33 = {"b1"};
		DBApp.select("pf", ConditionColumns33, ConditionColumnsValues33);
	
		String[] ConditionColumns34 = {"d"};
		String[] ConditionColumnsValues34 = {"d1"};
		DBApp.select("pf", ConditionColumns34, ConditionColumnsValues34);
	
		DBApp.select("pf");
	
		String[] ConditionColumns36 = {"f","e","g","h"};
		String[] ConditionColumnsValues36 = {"f2","e2","g2","h2"};
		DBApp.select("pf", ConditionColumns36, ConditionColumnsValues36);
	
		String[] ConditionColumns37 = {"c"};
		String[] ConditionColumnsValues37 = {"c1"};
		DBApp.select("pf", ConditionColumns37, ConditionColumnsValues37);
	
		DBApp.select("pf");
	
		DBApp.select("pf");
	
		int pageSelectPointer310 = genRandNum(pageCount3)-1;
		int recordSelectPointer310 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer310==pageCount3-1)
			recordSelectPointer310 = genRandNum((55%DBApp.dataPageSize))-1;
		DBApp.select("pf", pageSelectPointer310,recordSelectPointer310);
	
		DBApp.select("pf");
	
		String[] ConditionColumns312 = {"h"};
		String[] ConditionColumnsValues312 = {"h1"};
		DBApp.select("pf", ConditionColumns312, ConditionColumnsValues312);
	
		int pageSelectPointer313 = genRandNum(pageCount3)-1;
		int recordSelectPointer313 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer313==pageCount3-1)
			recordSelectPointer313 = genRandNum((55%DBApp.dataPageSize))-1;
		DBApp.select("pf", pageSelectPointer313,recordSelectPointer313);
	
		int pageSelectPointer314 = genRandNum(pageCount3)-1;
		int recordSelectPointer314 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer314==pageCount3-1)
			recordSelectPointer314 = genRandNum((55%DBApp.dataPageSize))-1;
		DBApp.select("pf", pageSelectPointer314,recordSelectPointer314);
	
		String[] ConditionColumns315 = {"d","a","g"};
		String[] ConditionColumnsValues315 = {"d0","a40","g5"};
		DBApp.select("pf", ConditionColumns315, ConditionColumnsValues315);
	
		DBApp.select("pf");
	
		DBApp.select("pf");
	
		String[] ConditionColumns318 = {"e"};
		String[] ConditionColumnsValues318 = {"e4"};
		DBApp.select("pf", ConditionColumns318, ConditionColumnsValues318);
	
		DBApp.select("pf");
	
		DBApp.select("pf");
	
		String[] ConditionColumns321 = {"c"};
		String[] ConditionColumnsValues321 = {"c0"};
		DBApp.select("pf", ConditionColumns321, ConditionColumnsValues321);
	
		DBApp.select("pf");
	
		int pageSelectPointer323 = genRandNum(pageCount3)-1;
		int recordSelectPointer323 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer323==pageCount3-1)
			recordSelectPointer323 = genRandNum((55%DBApp.dataPageSize))-1;
		DBApp.select("pf", pageSelectPointer323,recordSelectPointer323);
	
		DBApp.select("pf");
	
		int pageSelectPointer325 = genRandNum(pageCount3)-1;
		int recordSelectPointer325 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer325==pageCount3-1)
			recordSelectPointer325 = genRandNum((55%DBApp.dataPageSize))-1;
		DBApp.select("pf", pageSelectPointer325,recordSelectPointer325);
	
		String[] ConditionColumns326 = {"e"};
		String[] ConditionColumnsValues326 = {"e2"};
		DBApp.select("pf", ConditionColumns326, ConditionColumnsValues326);
	
		String selectTrace3 = DBApp.getFullTrace("pf");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 84);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 55"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d"};
		DBApp.createTable("rz1o", cols4);
		String [][] records_rz1o = new String[357][cols4.length];
		for(int i=0;i<357;i++)
		{
			records_rz1o[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_rz1o[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("rz1o", records_rz1o[i]);
		}
	
		int pageCount4 = (int)Math.ceil(357.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns40 = {"b"};
		String[] ConditionColumnsValues40 = {"b1"};
		DBApp.select("rz1o", ConditionColumns40, ConditionColumnsValues40);
	
		int pageSelectPointer41 = genRandNum(pageCount4)-1;
		int recordSelectPointer41 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer41==pageCount4-1)
			recordSelectPointer41 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer41,recordSelectPointer41);
	
		int pageSelectPointer42 = genRandNum(pageCount4)-1;
		int recordSelectPointer42 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer42==pageCount4-1)
			recordSelectPointer42 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer42,recordSelectPointer42);
	
		String[] ConditionColumns43 = {"c"};
		String[] ConditionColumnsValues43 = {"c2"};
		DBApp.select("rz1o", ConditionColumns43, ConditionColumnsValues43);
	
		DBApp.select("rz1o");
	
		int pageSelectPointer45 = genRandNum(pageCount4)-1;
		int recordSelectPointer45 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer45==pageCount4-1)
			recordSelectPointer45 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer45,recordSelectPointer45);
	
		DBApp.select("rz1o");
	
		int pageSelectPointer47 = genRandNum(pageCount4)-1;
		int recordSelectPointer47 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer47==pageCount4-1)
			recordSelectPointer47 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer47,recordSelectPointer47);
	
		DBApp.select("rz1o");
	
		String[] ConditionColumns49 = {"b"};
		String[] ConditionColumnsValues49 = {"b0"};
		DBApp.select("rz1o", ConditionColumns49, ConditionColumnsValues49);
	
		DBApp.select("rz1o");
	
		DBApp.select("rz1o");
	
		DBApp.select("rz1o");
	
		DBApp.select("rz1o");
	
		String[] ConditionColumns414 = {"d"};
		String[] ConditionColumnsValues414 = {"d2"};
		DBApp.select("rz1o", ConditionColumns414, ConditionColumnsValues414);
	
		DBApp.select("rz1o");
	
		String[] ConditionColumns416 = {"c","d"};
		String[] ConditionColumnsValues416 = {"c1","d3"};
		DBApp.select("rz1o", ConditionColumns416, ConditionColumnsValues416);
	
		int pageSelectPointer417 = genRandNum(pageCount4)-1;
		int recordSelectPointer417 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer417==pageCount4-1)
			recordSelectPointer417 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer417,recordSelectPointer417);
	
		String[] ConditionColumns418 = {"c"};
		String[] ConditionColumnsValues418 = {"c0"};
		DBApp.select("rz1o", ConditionColumns418, ConditionColumnsValues418);
	
		String[] ConditionColumns419 = {"c"};
		String[] ConditionColumnsValues419 = {"c2"};
		DBApp.select("rz1o", ConditionColumns419, ConditionColumnsValues419);
	
		int pageSelectPointer420 = genRandNum(pageCount4)-1;
		int recordSelectPointer420 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer420==pageCount4-1)
			recordSelectPointer420 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer420,recordSelectPointer420);
	
		DBApp.select("rz1o");
	
		int pageSelectPointer422 = genRandNum(pageCount4)-1;
		int recordSelectPointer422 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer422==pageCount4-1)
			recordSelectPointer422 = genRandNum((357%DBApp.dataPageSize))-1;
		DBApp.select("rz1o", pageSelectPointer422,recordSelectPointer422);
	
		String selectTrace4 = DBApp.getFullTrace("rz1o");
		String[] selectTraceArray4 = selectTrace4.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray4.length == 382);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray4[selectTraceArray4.length-1].equals("Pages Count: "+pageCount4+", Records Count: 357"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("dt6a4", cols0);
		String [][] records_dt6a4 = new String[425][cols0.length];
		for(int i=0;i<425;i++)
		{
			records_dt6a4[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_dt6a4[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("dt6a4", records_dt6a4[i]);
		}
	
		int pageCount0 = (int)Math.ceil(425.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		String[] ConditionColumns00 = {"l","j","k","h"};
		String[] ConditionColumnsValues00 = {"l8","j8","k5","h0"};
		DBApp.select("dt6a4", ConditionColumns00, ConditionColumnsValues00);
	
		String[] ConditionColumns01 = {"b","h","c","i"};
		String[] ConditionColumnsValues01 = {"b0","h0","c1","i7"};
		DBApp.select("dt6a4", ConditionColumns01, ConditionColumnsValues01);
	
		String[] ConditionColumns02 = {"c","h","d","b"};
		String[] ConditionColumnsValues02 = {"c2","h4","d0","b0"};
		DBApp.select("dt6a4", ConditionColumns02, ConditionColumnsValues02);
	
		int pageSelectPointer03 = genRandNum(pageCount0)-1;
		int recordSelectPointer03 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer03==pageCount0-1)
			recordSelectPointer03 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer03,recordSelectPointer03);
	
		DBApp.select("dt6a4");
	
		String[] ConditionColumns05 = {"e","l"};
		String[] ConditionColumnsValues05 = {"e2","l0"};
		DBApp.select("dt6a4", ConditionColumns05, ConditionColumnsValues05);
	
		String[] ConditionColumns06 = {"d"};
		String[] ConditionColumnsValues06 = {"d0"};
		DBApp.select("dt6a4", ConditionColumns06, ConditionColumnsValues06);
	
		int pageSelectPointer07 = genRandNum(pageCount0)-1;
		int recordSelectPointer07 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer07==pageCount0-1)
			recordSelectPointer07 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer07,recordSelectPointer07);
	
		DBApp.select("dt6a4");
	
		String[] ConditionColumns09 = {"i","l","e"};
		String[] ConditionColumnsValues09 = {"i3","l9","e3"};
		DBApp.select("dt6a4", ConditionColumns09, ConditionColumnsValues09);
	
		int pageSelectPointer010 = genRandNum(pageCount0)-1;
		int recordSelectPointer010 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer010==pageCount0-1)
			recordSelectPointer010 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer010,recordSelectPointer010);
	
		String[] ConditionColumns011 = {"k"};
		String[] ConditionColumnsValues011 = {"k0"};
		DBApp.select("dt6a4", ConditionColumns011, ConditionColumnsValues011);
	
		int pageSelectPointer012 = genRandNum(pageCount0)-1;
		int recordSelectPointer012 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer012==pageCount0-1)
			recordSelectPointer012 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer012,recordSelectPointer012);
	
		int pageSelectPointer013 = genRandNum(pageCount0)-1;
		int recordSelectPointer013 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer013==pageCount0-1)
			recordSelectPointer013 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer013,recordSelectPointer013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"i"};
		String[] ConditionColumnsValues015 = {"i5"};
		DBApp.select("dt6a4", ConditionColumns015, ConditionColumnsValues015);
	
		DBApp.select("dt6a4");
	
		int pageSelectPointer017 = genRandNum(pageCount0)-1;
		int recordSelectPointer017 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer017==pageCount0-1)
			recordSelectPointer017 = genRandNum((425%DBApp.dataPageSize))-1;
		DBApp.select("dt6a4", pageSelectPointer017,recordSelectPointer017);
	
		String[] ConditionColumns018 = {"e"};
		String[] ConditionColumnsValues018 = {"e3"};
		DBApp.select("dt6a4", ConditionColumns018, ConditionColumnsValues018);
	
		String selectTrace0 = DBApp.getFullTrace("dt6a4");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 446);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 425"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wo6", cols1);
		String [][] records_wo6 = new String[348][cols1.length];
		for(int i=0;i<348;i++)
		{
			records_wo6[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_wo6[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("wo6", records_wo6[i]);
		}
	
		int pageCount1 = (int)Math.ceil(348.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns10 = {"g"};
		String[] ConditionColumnsValues10 = {"g4"};
		DBApp.select("wo6", ConditionColumns10, ConditionColumnsValues10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer12,recordSelectPointer12);
	
		int pageSelectPointer13 = genRandNum(pageCount1)-1;
		int recordSelectPointer13 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer13==pageCount1-1)
			recordSelectPointer13 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer13,recordSelectPointer13);
	
		int pageSelectPointer14 = genRandNum(pageCount1)-1;
		int recordSelectPointer14 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer14==pageCount1-1)
			recordSelectPointer14 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer14,recordSelectPointer14);
	
		DBApp.select("wo6");
	
		String[] ConditionColumns16 = {"c"};
		String[] ConditionColumnsValues16 = {"c0"};
		DBApp.select("wo6", ConditionColumns16, ConditionColumnsValues16);
	
		DBApp.select("wo6");
	
		String[] ConditionColumns18 = {"d"};
		String[] ConditionColumnsValues18 = {"d1"};
		DBApp.select("wo6", ConditionColumns18, ConditionColumnsValues18);
	
		DBApp.select("wo6");
	
		int pageSelectPointer110 = genRandNum(pageCount1)-1;
		int recordSelectPointer110 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer110==pageCount1-1)
			recordSelectPointer110 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer110,recordSelectPointer110);
	
		int pageSelectPointer111 = genRandNum(pageCount1)-1;
		int recordSelectPointer111 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer111==pageCount1-1)
			recordSelectPointer111 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer111,recordSelectPointer111);
	
		int pageSelectPointer112 = genRandNum(pageCount1)-1;
		int recordSelectPointer112 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer112==pageCount1-1)
			recordSelectPointer112 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer112,recordSelectPointer112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer113,recordSelectPointer113);
	
		DBApp.select("wo6");
	
		int pageSelectPointer115 = genRandNum(pageCount1)-1;
		int recordSelectPointer115 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer115==pageCount1-1)
			recordSelectPointer115 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer115,recordSelectPointer115);
	
		int pageSelectPointer116 = genRandNum(pageCount1)-1;
		int recordSelectPointer116 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer116==pageCount1-1)
			recordSelectPointer116 = genRandNum((348%DBApp.dataPageSize))-1;
		DBApp.select("wo6", pageSelectPointer116,recordSelectPointer116);
	
		DBApp.select("wo6");
	
		String[] ConditionColumns118 = {"f","d","g"};
		String[] ConditionColumnsValues118 = {"f5","d3","g6"};
		DBApp.select("wo6", ConditionColumns118, ConditionColumnsValues118);
	
		String[] ConditionColumns119 = {"g"};
		String[] ConditionColumnsValues119 = {"g6"};
		DBApp.select("wo6", ConditionColumns119, ConditionColumnsValues119);
	
		DBApp.select("wo6");
	
		String[] ConditionColumns121 = {"c"};
		String[] ConditionColumnsValues121 = {"c1"};
		DBApp.select("wo6", ConditionColumns121, ConditionColumnsValues121);
	
		String[] ConditionColumns122 = {"b","g"};
		String[] ConditionColumnsValues122 = {"b0","g1"};
		DBApp.select("wo6", ConditionColumns122, ConditionColumnsValues122);
	
		String selectTrace1 = DBApp.getFullTrace("wo6");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 373);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 348"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ko", cols2);
		String [][] records_ko = new String[457][cols2.length];
		for(int i=0;i<457;i++)
		{
			records_ko[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ko[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ko", records_ko[i]);
		}
	
		int pageCount2 = (int)Math.ceil(457.0/DBApp.dataPageSize);
	
		 //performing 16 selects:
	
		DBApp.select("ko");
	
		int pageSelectPointer21 = genRandNum(pageCount2)-1;
		int recordSelectPointer21 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer21==pageCount2-1)
			recordSelectPointer21 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ko", pageSelectPointer21,recordSelectPointer21);
	
		DBApp.select("ko");
	
		DBApp.select("ko");
	
		int pageSelectPointer24 = genRandNum(pageCount2)-1;
		int recordSelectPointer24 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer24==pageCount2-1)
			recordSelectPointer24 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ko", pageSelectPointer24,recordSelectPointer24);
	
		DBApp.select("ko");
	
		String[] ConditionColumns26 = {"i"};
		String[] ConditionColumnsValues26 = {"i2"};
		DBApp.select("ko", ConditionColumns26, ConditionColumnsValues26);
	
		String[] ConditionColumns27 = {"i"};
		String[] ConditionColumnsValues27 = {"i7"};
		DBApp.select("ko", ConditionColumns27, ConditionColumnsValues27);
	
		String[] ConditionColumns28 = {"f"};
		String[] ConditionColumnsValues28 = {"f0"};
		DBApp.select("ko", ConditionColumns28, ConditionColumnsValues28);
	
		int pageSelectPointer29 = genRandNum(pageCount2)-1;
		int recordSelectPointer29 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer29==pageCount2-1)
			recordSelectPointer29 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ko", pageSelectPointer29,recordSelectPointer29);
	
		DBApp.select("ko");
	
		int pageSelectPointer211 = genRandNum(pageCount2)-1;
		int recordSelectPointer211 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer211==pageCount2-1)
			recordSelectPointer211 = genRandNum((457%DBApp.dataPageSize))-1;
		DBApp.select("ko", pageSelectPointer211,recordSelectPointer211);
	
		DBApp.select("ko");
	
		DBApp.select("ko");
	
		DBApp.select("ko");
	
		String[] ConditionColumns215 = {"e"};
		String[] ConditionColumnsValues215 = {"e3"};
		DBApp.select("ko", ConditionColumns215, ConditionColumnsValues215);
	
		String selectTrace2 = DBApp.getFullTrace("ko");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 475);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 457"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableFullTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("s580", cols0);
		String [][] records_s580 = new String[211][cols0.length];
		for(int i=0;i<211;i++)
		{
			records_s580[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_s580[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("s580", records_s580[i]);
		}
	
		int pageCount0 = (int)Math.ceil(211.0/DBApp.dataPageSize);
	
		 //performing 23 selects:
	
		String[] ConditionColumns00 = {"b","e"};
		String[] ConditionColumnsValues00 = {"b0","e0"};
		DBApp.select("s580", ConditionColumns00, ConditionColumnsValues00);
	
		DBApp.select("s580");
	
		DBApp.select("s580");
	
		DBApp.select("s580");
	
		String[] ConditionColumns04 = {"d","e"};
		String[] ConditionColumnsValues04 = {"d3","e4"};
		DBApp.select("s580", ConditionColumns04, ConditionColumnsValues04);
	
		int pageSelectPointer05 = genRandNum(pageCount0)-1;
		int recordSelectPointer05 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer05==pageCount0-1)
			recordSelectPointer05 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("s580", pageSelectPointer05,recordSelectPointer05);
	
		DBApp.select("s580");
	
		DBApp.select("s580");
	
		DBApp.select("s580");
	
		DBApp.select("s580");
	
		DBApp.select("s580");
	
		int pageSelectPointer011 = genRandNum(pageCount0)-1;
		int recordSelectPointer011 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer011==pageCount0-1)
			recordSelectPointer011 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("s580", pageSelectPointer011,recordSelectPointer011);
	
		DBApp.select("s580");
	
		String[] ConditionColumns013 = {"d"};
		String[] ConditionColumnsValues013 = {"d3"};
		DBApp.select("s580", ConditionColumns013, ConditionColumnsValues013);
	
		int pageSelectPointer014 = genRandNum(pageCount0)-1;
		int recordSelectPointer014 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer014==pageCount0-1)
			recordSelectPointer014 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("s580", pageSelectPointer014,recordSelectPointer014);
	
		String[] ConditionColumns015 = {"d"};
		String[] ConditionColumnsValues015 = {"d1"};
		DBApp.select("s580", ConditionColumns015, ConditionColumnsValues015);
	
		int pageSelectPointer016 = genRandNum(pageCount0)-1;
		int recordSelectPointer016 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer016==pageCount0-1)
			recordSelectPointer016 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("s580", pageSelectPointer016,recordSelectPointer016);
	
		String[] ConditionColumns017 = {"e"};
		String[] ConditionColumnsValues017 = {"e2"};
		DBApp.select("s580", ConditionColumns017, ConditionColumnsValues017);
	
		DBApp.select("s580");
	
		String[] ConditionColumns019 = {"b","a"};
		String[] ConditionColumnsValues019 = {"b1","a65"};
		DBApp.select("s580", ConditionColumns019, ConditionColumnsValues019);
	
		String[] ConditionColumns020 = {"c"};
		String[] ConditionColumnsValues020 = {"c2"};
		DBApp.select("s580", ConditionColumns020, ConditionColumnsValues020);
	
		int pageSelectPointer021 = genRandNum(pageCount0)-1;
		int recordSelectPointer021 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer021==pageCount0-1)
			recordSelectPointer021 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("s580", pageSelectPointer021,recordSelectPointer021);
	
		int pageSelectPointer022 = genRandNum(pageCount0)-1;
		int recordSelectPointer022 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer022==pageCount0-1)
			recordSelectPointer022 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("s580", pageSelectPointer022,recordSelectPointer022);
	
		String selectTrace0 = DBApp.getFullTrace("s580");
		String[] selectTraceArray0 = selectTrace0.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray0.length == 236);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray0[selectTraceArray0.length-1].equals("Pages Count: "+pageCount0+", Records Count: 211"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("g1q", cols1);
		String [][] records_g1q = new String[286][cols1.length];
		for(int i=0;i<286;i++)
		{
			records_g1q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_g1q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("g1q", records_g1q[i]);
		}
	
		int pageCount1 = (int)Math.ceil(286.0/DBApp.dataPageSize);
	
		 //performing 22 selects:
	
		int pageSelectPointer10 = genRandNum(pageCount1)-1;
		int recordSelectPointer10 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer10==pageCount1-1)
			recordSelectPointer10 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer10,recordSelectPointer10);
	
		int pageSelectPointer11 = genRandNum(pageCount1)-1;
		int recordSelectPointer11 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer11==pageCount1-1)
			recordSelectPointer11 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer11,recordSelectPointer11);
	
		int pageSelectPointer12 = genRandNum(pageCount1)-1;
		int recordSelectPointer12 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer12==pageCount1-1)
			recordSelectPointer12 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer12,recordSelectPointer12);
	
		DBApp.select("g1q");
	
		String[] ConditionColumns14 = {"g","d","f","a"};
		String[] ConditionColumnsValues14 = {"g5","d0","f0","a96"};
		DBApp.select("g1q", ConditionColumns14, ConditionColumnsValues14);
	
		DBApp.select("g1q");
	
		int pageSelectPointer16 = genRandNum(pageCount1)-1;
		int recordSelectPointer16 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer16==pageCount1-1)
			recordSelectPointer16 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer16,recordSelectPointer16);
	
		int pageSelectPointer17 = genRandNum(pageCount1)-1;
		int recordSelectPointer17 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer17==pageCount1-1)
			recordSelectPointer17 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer17,recordSelectPointer17);
	
		int pageSelectPointer18 = genRandNum(pageCount1)-1;
		int recordSelectPointer18 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer18==pageCount1-1)
			recordSelectPointer18 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer18,recordSelectPointer18);
	
		DBApp.select("g1q");
	
		String[] ConditionColumns110 = {"c"};
		String[] ConditionColumnsValues110 = {"c1"};
		DBApp.select("g1q", ConditionColumns110, ConditionColumnsValues110);
	
		String[] ConditionColumns111 = {"e"};
		String[] ConditionColumnsValues111 = {"e1"};
		DBApp.select("g1q", ConditionColumns111, ConditionColumnsValues111);
	
		String[] ConditionColumns112 = {"e","c"};
		String[] ConditionColumnsValues112 = {"e3","c0"};
		DBApp.select("g1q", ConditionColumns112, ConditionColumnsValues112);
	
		int pageSelectPointer113 = genRandNum(pageCount1)-1;
		int recordSelectPointer113 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer113==pageCount1-1)
			recordSelectPointer113 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer113,recordSelectPointer113);
	
		String[] ConditionColumns114 = {"d"};
		String[] ConditionColumnsValues114 = {"d3"};
		DBApp.select("g1q", ConditionColumns114, ConditionColumnsValues114);
	
		String[] ConditionColumns115 = {"c"};
		String[] ConditionColumnsValues115 = {"c0"};
		DBApp.select("g1q", ConditionColumns115, ConditionColumnsValues115);
	
		DBApp.select("g1q");
	
		DBApp.select("g1q");
	
		DBApp.select("g1q");
	
		DBApp.select("g1q");
	
		int pageSelectPointer120 = genRandNum(pageCount1)-1;
		int recordSelectPointer120 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer120==pageCount1-1)
			recordSelectPointer120 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer120,recordSelectPointer120);
	
		int pageSelectPointer121 = genRandNum(pageCount1)-1;
		int recordSelectPointer121 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer121==pageCount1-1)
			recordSelectPointer121 = genRandNum((286%DBApp.dataPageSize))-1;
		DBApp.select("g1q", pageSelectPointer121,recordSelectPointer121);
	
		String selectTrace1 = DBApp.getFullTrace("g1q");
		String[] selectTraceArray1 = selectTrace1.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray1.length == 310);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray1[selectTraceArray1.length-1].equals("Pages Count: "+pageCount1+", Records Count: 286"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("epp1l", cols2);
		String [][] records_epp1l = new String[211][cols2.length];
		for(int i=0;i<211;i++)
		{
			records_epp1l[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_epp1l[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("epp1l", records_epp1l[i]);
		}
	
		int pageCount2 = (int)Math.ceil(211.0/DBApp.dataPageSize);
	
		 //performing 19 selects:
	
		String[] ConditionColumns20 = {"c"};
		String[] ConditionColumnsValues20 = {"c2"};
		DBApp.select("epp1l", ConditionColumns20, ConditionColumnsValues20);
	
		String[] ConditionColumns21 = {"c"};
		String[] ConditionColumnsValues21 = {"c2"};
		DBApp.select("epp1l", ConditionColumns21, ConditionColumnsValues21);
	
		int pageSelectPointer22 = genRandNum(pageCount2)-1;
		int recordSelectPointer22 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer22==pageCount2-1)
			recordSelectPointer22 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("epp1l", pageSelectPointer22,recordSelectPointer22);
	
		int pageSelectPointer23 = genRandNum(pageCount2)-1;
		int recordSelectPointer23 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer23==pageCount2-1)
			recordSelectPointer23 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("epp1l", pageSelectPointer23,recordSelectPointer23);
	
		String[] ConditionColumns24 = {"b"};
		String[] ConditionColumnsValues24 = {"b0"};
		DBApp.select("epp1l", ConditionColumns24, ConditionColumnsValues24);
	
		DBApp.select("epp1l");
	
		String[] ConditionColumns26 = {"b"};
		String[] ConditionColumnsValues26 = {"b0"};
		DBApp.select("epp1l", ConditionColumns26, ConditionColumnsValues26);
	
		int pageSelectPointer27 = genRandNum(pageCount2)-1;
		int recordSelectPointer27 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer27==pageCount2-1)
			recordSelectPointer27 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("epp1l", pageSelectPointer27,recordSelectPointer27);
	
		String[] ConditionColumns28 = {"c"};
		String[] ConditionColumnsValues28 = {"c0"};
		DBApp.select("epp1l", ConditionColumns28, ConditionColumnsValues28);
	
		DBApp.select("epp1l");
	
		DBApp.select("epp1l");
	
		DBApp.select("epp1l");
	
		DBApp.select("epp1l");
	
		int pageSelectPointer213 = genRandNum(pageCount2)-1;
		int recordSelectPointer213 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer213==pageCount2-1)
			recordSelectPointer213 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("epp1l", pageSelectPointer213,recordSelectPointer213);
	
		DBApp.select("epp1l");
	
		int pageSelectPointer215 = genRandNum(pageCount2)-1;
		int recordSelectPointer215 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer215==pageCount2-1)
			recordSelectPointer215 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("epp1l", pageSelectPointer215,recordSelectPointer215);
	
		DBApp.select("epp1l");
	
		int pageSelectPointer217 = genRandNum(pageCount2)-1;
		int recordSelectPointer217 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer217==pageCount2-1)
			recordSelectPointer217 = genRandNum((211%DBApp.dataPageSize))-1;
		DBApp.select("epp1l", pageSelectPointer217,recordSelectPointer217);
	
		DBApp.select("epp1l");
	
		String selectTrace2 = DBApp.getFullTrace("epp1l");
		String[] selectTraceArray2 = selectTrace2.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray2.length == 232);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray2[selectTraceArray2.length-1].equals("Pages Count: "+pageCount2+", Records Count: 211"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("jfg", cols3);
		String [][] records_jfg = new String[142][cols3.length];
		for(int i=0;i<142;i++)
		{
			records_jfg[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_jfg[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("jfg", records_jfg[i]);
		}
	
		int pageCount3 = (int)Math.ceil(142.0/DBApp.dataPageSize);
	
		 //performing 29 selects:
	
		int pageSelectPointer30 = genRandNum(pageCount3)-1;
		int recordSelectPointer30 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer30==pageCount3-1)
			recordSelectPointer30 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer30,recordSelectPointer30);
	
		int pageSelectPointer31 = genRandNum(pageCount3)-1;
		int recordSelectPointer31 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer31==pageCount3-1)
			recordSelectPointer31 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer31,recordSelectPointer31);
	
		DBApp.select("jfg");
	
		String[] ConditionColumns33 = {"c","e","k","m","b","g"};
		String[] ConditionColumnsValues33 = {"c0","e2","k0","m2","b0","g6"};
		DBApp.select("jfg", ConditionColumns33, ConditionColumnsValues33);
	
		String[] ConditionColumns34 = {"g"};
		String[] ConditionColumnsValues34 = {"g3"};
		DBApp.select("jfg", ConditionColumns34, ConditionColumnsValues34);
	
		DBApp.select("jfg");
	
		int pageSelectPointer36 = genRandNum(pageCount3)-1;
		int recordSelectPointer36 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer36==pageCount3-1)
			recordSelectPointer36 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer36,recordSelectPointer36);
	
		DBApp.select("jfg");
	
		int pageSelectPointer38 = genRandNum(pageCount3)-1;
		int recordSelectPointer38 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer38==pageCount3-1)
			recordSelectPointer38 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer38,recordSelectPointer38);
	
		int pageSelectPointer39 = genRandNum(pageCount3)-1;
		int recordSelectPointer39 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer39==pageCount3-1)
			recordSelectPointer39 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer39,recordSelectPointer39);
	
		String[] ConditionColumns310 = {"l","k"};
		String[] ConditionColumnsValues310 = {"l5","k9"};
		DBApp.select("jfg", ConditionColumns310, ConditionColumnsValues310);
	
		int pageSelectPointer311 = genRandNum(pageCount3)-1;
		int recordSelectPointer311 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer311==pageCount3-1)
			recordSelectPointer311 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer311,recordSelectPointer311);
	
		DBApp.select("jfg");
	
		DBApp.select("jfg");
	
		String[] ConditionColumns314 = {"l","e","j","i","m"};
		String[] ConditionColumnsValues314 = {"l8","e0","j0","i2","m7"};
		DBApp.select("jfg", ConditionColumns314, ConditionColumnsValues314);
	
		DBApp.select("jfg");
	
		int pageSelectPointer316 = genRandNum(pageCount3)-1;
		int recordSelectPointer316 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer316==pageCount3-1)
			recordSelectPointer316 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer316,recordSelectPointer316);
	
		int pageSelectPointer317 = genRandNum(pageCount3)-1;
		int recordSelectPointer317 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer317==pageCount3-1)
			recordSelectPointer317 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer317,recordSelectPointer317);
	
		DBApp.select("jfg");
	
		DBApp.select("jfg");
	
		int pageSelectPointer320 = genRandNum(pageCount3)-1;
		int recordSelectPointer320 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer320==pageCount3-1)
			recordSelectPointer320 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer320,recordSelectPointer320);
	
		int pageSelectPointer321 = genRandNum(pageCount3)-1;
		int recordSelectPointer321 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer321==pageCount3-1)
			recordSelectPointer321 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer321,recordSelectPointer321);
	
		int pageSelectPointer322 = genRandNum(pageCount3)-1;
		int recordSelectPointer322 = genRandNum(DBApp.dataPageSize)-1;
		if(pageSelectPointer322==pageCount3-1)
			recordSelectPointer322 = genRandNum((142%DBApp.dataPageSize))-1;
		DBApp.select("jfg", pageSelectPointer322,recordSelectPointer322);
	
		String[] ConditionColumns323 = {"c","l","f","e","a","j"};
		String[] ConditionColumnsValues323 = {"c2","l8","f2","e3","a8","j8"};
		DBApp.select("jfg", ConditionColumns323, ConditionColumnsValues323);
	
		String[] ConditionColumns324 = {"b","a"};
		String[] ConditionColumnsValues324 = {"b1","a87"};
		DBApp.select("jfg", ConditionColumns324, ConditionColumnsValues324);
	
		String[] ConditionColumns325 = {"d"};
		String[] ConditionColumnsValues325 = {"d1"};
		DBApp.select("jfg", ConditionColumns325, ConditionColumnsValues325);
	
		DBApp.select("jfg");
	
		String[] ConditionColumns327 = {"e"};
		String[] ConditionColumnsValues327 = {"e2"};
		DBApp.select("jfg", ConditionColumns327, ConditionColumnsValues327);
	
		DBApp.select("jfg");
	
		String selectTrace3 = DBApp.getFullTrace("jfg");
		String[] selectTraceArray3 = selectTrace3.split("\n");
		assertTrue("Full trace should show the correct number of operations performed on a table.", selectTraceArray3.length == 173);
		assertTrue("Last line in the full trace should report the number of pages and records correctly.", selectTraceArray3[selectTraceArray3.length-1].equals("Pages Count: "+pageCount3+", Records Count: 142"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(654);
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